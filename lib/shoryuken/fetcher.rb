module Shoryuken
  class Fetcher
    include Util

    FETCH_LIMIT = 10

    def fetch(queue, available_processors)
      started_at = Time.now

      logger.debug { "Looking for new messages in '#{queue}'" }

      begin
        batch_by_interval = Shoryuken.worker_registry.batch_by_interval(queue)

        limit = available_processors > FETCH_LIMIT ? FETCH_LIMIT : available_processors

        sqs_msgs = Array(receive_messages(queue, limit))

        if batch_by_interval > 0
					logger.debug { "Waiting for messages for #{batch_by_interval}sec" }

					stop_polling_at = started_at + batch_by_interval
					while Time.now < stop_polling_at
						fetch = Array(receive_messages(queue, limit))
						if fetch.any?
							sqs_msgs.concat(fetch)
						else
							sleep 1
						end
						logger.debug { "Batch by interval buffer has #{sqs_msgs.length} messages, #{stop_polling_at - Time.now}sec remaining" }
					end

					logger.debug { "Finished waiting for messages for #{batch_by_interval}sec" }
				end

				if sqs_msgs.any?
					logger.info { "Found #{sqs_msgs.size} messages for '#{queue.name}'" } unless sqs_msgs.empty?
					logger.debug { "Fetcher for '#{queue}' completed in #{elapsed(started_at)} ms" }
					sqs_msgs
				end
      rescue => ex
        logger.error { "Error fetching message: #{ex}" }
        logger.error { ex.backtrace.first }
        []
      end
    end

    private

    def receive_messages(queue, limit)
      # AWS limits the batch size by 10
      limit = limit > FETCH_LIMIT ? FETCH_LIMIT : limit

      options = Shoryuken.sqs_client_receive_message_opts.to_h.dup
      options[:max_number_of_messages] = limit
      options[:message_attribute_names] = %w(All)
      options[:attribute_names] = %w(All)

      options.merge!(queue.options)

      Shoryuken::Client.queues(queue.name).receive_messages(options)
    end
  end
end
