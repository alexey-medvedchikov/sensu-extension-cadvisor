
require 'rubygems' if RUBY_VERSION < '1.9.0'

require 'net/http'
require 'json'
require 'uri'
require 'sensu/transport'

module Sensu
  module Extension
    class Cadvisor < Check
      def name
        'cadvisor'
      end

      def description
        'collects docker system metrics via cAdvisor'
      end

      def options
        return @options if @options
        @options = {
          etcd: 'http://localhost:4001',
          etcd_version: 'v1',
          endpoints: 'http://localhost:8080',
          mode: 'endpoint', # or etcd
          subcontainers: '.+',
          timeout: 2
        }
        if @settings[:cadvisor].is_a?(Hash)
          @options.merge!(@settings[:cadvisor])
        end
        @options
      end

      def definition
        {
          type: 'metric',
          name: name,
          standalone: false,
          mutator: 'ruby_hash'
        }
      end

      def run(check)
        if options[:mode] == 'endpoint'
          endpoints = options[:endpoints].split(',')
        else
          endpoints = []
          options[:etcd].split(',').each do |srv|
            etcd_uri = "#{srv}/#{options[:etcd_version]}/machines"
            begin
              timeout options[:timeout] do
                machines_page = Net::HTTP.get(URI(etcd_uri))
                machines_page.split(',').each do |endpoint|
                  endpoints << "http://#{URI(endpoint.strip).host}:8080"
                end
              end
              break
            rescue Timeout::Error
             @logger.error("cadvisor: etcd endpoint #{srv} timed out")
            rescue => exc
             @logger.error("cadvisor: etcd endpoint #{srv} unavailable")
            end
          end
        end
        endpoints.each do |endpoint|
          begin
            cadvisor_parse(endpoint, check)
          rescue => exc
            @logger.error("cadvisor: cadvisor endpoint #{endpoint} unavailable")
            @logger.error(exc)
            @logger.error(exc.backtrace)
         end
        end
        yield "\n", 0
      end

      private

      def cadvisor_parse(endpoint, check)
        host = URI(endpoint).host
        storage_uri = "#{endpoint}/api/v2.0/storage/"
        data = get_json_page(storage_uri)
        cadvisor_parse_storage(data).each do |metrics|
          metrics['machine'] = host
          tags = ['sensu', 'cadvisor', 'storage', 'metrics']
          event_generate(check, metrics, tags)
        end
  
        docker_uri = "#{endpoint}/api/v1.3/docker/"
        data = get_json_page(docker_uri)
        cadvisor_parse_docker(data).each do |metrics|
          metrics['machine'] = host
          tags = ['sensu', 'cadvisor', 'docker', 'metrics']
          event_generate(check, metrics, tags)
        end
  
        container_uri = "#{endpoint}/api/v1.3/containers/"
        data = get_json_page(container_uri)
        metrics = cadvisor_parse_container(data)
        metrics['machine'] = host
        tags = ['sensu', 'cadvisor', 'containers', 'metrics']
        event_generate(check, metrics, tags)
  
        subcontainers_uri = "#{endpoint}/api/v1.3/subcontainers/"
        data = get_json_page(subcontainers_uri)
        cadvisor_parse_subcontainers(data, options[:subcontainers]).each do |metrics|
          metrics['machine'] = host
          tags = ['sensu', 'cadvisor', 'subcontainers', 'metrics']
          event_generate(check, metrics, tags)
        end
  
        machine_uri = "#{endpoint}/api/v2.0/machine/"
        data = get_json_page(machine_uri)
        metrics = cadvisor_parse_machine(data)
        metrics['machine'] = host
        tags = ['sensu', 'cadvisor', 'machine', 'metrics']
        event_generate(check, metrics, tags)
      end

      def pretty_metrics(stats)
        metrics = Hash.new
        stats.sort! { |x, y| y['timestamp'] <=> x['timestamp'] }
        ts_new = Time.strptime(stats[0]['timestamp'], '%Y-%m-%dT%H:%M:%S').to_i
        ts_old = Time.strptime(stats[1]['timestamp'], '%Y-%m-%dT%H:%M:%S').to_i
        ts_delta = ts_new - ts_old

        begin
          metrics['cpu'] = Hash.new
          metrics['cpu']['loadavg'] = stats[0]['cpu']['load_average']
          # This equals to (stats[0]['cpu']['usage'][METRIC] \
          #   - stats[1]['cpu']['usage'][METRIC]).to_f / 100 / 1_000_000 / ts_delta
          get_stats = lambda { |x, y| stats[x]['cpu']['usage'][y] }
          diff_stats = lambda { |x| (get_stats[0, x] - get_stats[1, x]).to_f / 100_000_000 / ts_delta }
          %w(total user system).each do |metric|
            metrics['cpu'][metric] = diff_stats[metric]
          end
        rescue
        end

        begin
          metrics['memory'] = Hash.new
          %w(usage working_set).each do |metric|
            metrics['memory'][metric] = stats[0]['memory'][metric]
          end
          %w(pgfault pgmajfault).each do |metric|
            metrics['memory'][metric] = stats[0]['memory']['container_data'][metric]
          end
        rescue
        end

        begin
          metrics['tasks'] = Hash.new
          %w(sleeping running stopped uninterruptible io_wait).each do |metric|
            metrics['tasks'][metric] = stats[0]['task_stats']["nr_#{metric}"]
          end
        rescue
        end

        begin
          metrics['network'] = Hash.new
          # This equals to (stats[0]['network'][DIRECTION_METRIC] \
          #   - stats[1]['network'][DIRECTION_METRIC]).to_f / ts_delta
          get_stats = lambda { |x, y| stats[x]['network'][y] }
          diff_stats = lambda { |x| (get_stats[0, x] - get_stats[1, x]).to_f / ts_delta }
          %w(rx tx).each do |direction|
            %w(bytes packets errors dropped).each do |metric|
              metric_name = "#{direction}_#{metric}"
              metrics['network'][metric_name] = diff_stats[metric_name]
            end
          end
        rescue
        end

        begin
          metrics['diskio'] = Hash.new
          # This equals to (stats[0]['diskio']['io_service_bytes'][0]['stats'][METRIC] \
          #   - stats[1]['diskio']['io_service_bytes'][0]['stats'][METRIC]).to_f / ts_delta
          get_stats = lambda { |x, y| stats[x]['diskio']['io_service_bytes'][0]['stats'][y] }
          diff_stats = lambda { |x| (get_stats[0, x] - get_stats[1, x]).to_f / ts_delta }
          %w(Async Sync Read Write Total).each do |k|
            metrics['diskio'][k.downcase] = diff_stats[k]
          end
        rescue
        end
        metrics
      end

      def event_generate(check, obj, tags)
        event = {
          client: @settings[:client][:name],
          check: check.merge({
            status: 0,
            output: JSON.dump(obj),
            tags: tags
          })
        }
        if @transport == nil
          transport_name = @settings[:transport][:name]
          @transport = Transport.connect(transport_name, @settings[transport_name])
          @transport.on_error do |error|
            @logger.fatal("transport connection error", :error => error.to_s)
            if @settings[:transport][:reconnect_on_error]
              @transport.reconnect
            else
              stop
            end
          end
        end
        @transport.publish(:direct, 'results', JSON.dump(event))
      end

      def get_json_page(uri)
        page_source = Net::HTTP.get(URI(uri))
        JSON.load(page_source)
      end

      def cadvisor_parse_machine(data)
        metrics = Hash.new
        %w{num_cores cpu_frequency_khz memory_capacity}.each do |key|
          metrics[key] = data[key]
        end
        metrics
      end

      def cadvisor_parse_storage(data)
        all_metrics = Array.new
        data.each do |elm|
          metrics = Hash.new
          metrics['disk'] = elm
          all_metrics.push(metrics)
        end
        all_metrics
      end

      def cadvisor_parse_subcontainers(data, filters)
        filter = Regexp.new("(#{filters.join(')|(')})")
        all_metrics = Array.new
        data.select { |x| filter.match(x['name']) }.each do |subcontainer|
          metrics = Hash.new
          metrics['subcontainer'] = subcontainer['name']
          metrics.merge!(pretty_metrics(subcontainer['stats']))
          all_metrics.push(metrics)
        end
        all_metrics
      end

      def cadvisor_parse_docker(data)
        all_metrics = Array.new
        data.each_pair do |k, v|
          metrics = Hash.new
          metrics['container'] = v['aliases'][0]
          metrics.merge!(pretty_metrics(v['stats']))
          all_metrics.push(metrics)
        end
        all_metrics
      end

      def cadvisor_parse_container(data)
        metrics = Hash.new
        metrics['container'] = data['name']
        metrics.merge!(pretty_metrics(data['stats']))
        metrics
      end
    end
  end
end
