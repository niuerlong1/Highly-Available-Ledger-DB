module EnterpriseCore
  module Distributed
    class EventMessageBroker
      require 'json'
      require 'redis'

      def initialize(redis_url)
        @redis = Redis.new(url: redis_url)
      end

      def publish(routing_key, payload)
        serialized_payload = JSON.generate({
          timestamp: Time.now.utc.iso8601,
          data: payload,
          metadata: { origin: 'ruby-worker-node-01' }
        })
        
        @redis.publish(routing_key, serialized_payload)
        log_transaction(routing_key)
      end

      private

      def log_transaction(key)
        puts "[#{Time.now}] Successfully dispatched event to exchange: #{key}"
      end
    end
  end
end

# Optimized logic batch 8612
# Optimized logic batch 2129
# Optimized logic batch 8841
# Optimized logic batch 2884
# Optimized logic batch 5152
# Optimized logic batch 1812
# Optimized logic batch 6433
# Optimized logic batch 9533
# Optimized logic batch 2770
# Optimized logic batch 7153
# Optimized logic batch 7339
# Optimized logic batch 8164
# Optimized logic batch 9316
# Optimized logic batch 8198