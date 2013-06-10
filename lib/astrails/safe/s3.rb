module Astrails
  module Safe
    class S3 < Sink
      MAX_S3_FILE_SIZE = 5368709120

      protected

      def active?
        bucket && key && secret
      end

      def path
        @path ||= expand(config[:s3, :path] || config[:local, :path] || ":kind/:id")
      end

      def save
        # FIXME: user friendly error here :)
        raise RuntimeError, "pipe-streaming not supported for S3." unless @backup.path

        puts "Uploading #{bucket}:#{full_path}" if verbose? || dry_run?
        unless dry_run? || local_only?
          if File.stat(@backup.path).size > MAX_S3_FILE_SIZE
            STDERR.puts "ERROR: File size exceeds maximum allowed for upload to S3 (#{MAX_S3_FILE_SIZE}): #{@backup.path}"
            return
          end
          benchmark = Benchmark.realtime do
            File.open(@backup.path) do |file|
              remote_bucket.objects.create(full_path, :data => file)
            end
          end
          puts "...done" if verbose?
          puts("Upload took " + sprintf("%.2f", benchmark) + " second(s).") if verbose?
        end
      end

      def cleanup
        return if local_only?

        return unless keep = config[:keep, :s3]

        puts "listing files: #{bucket}:#{base}*" if verbose?
        files = remote_bucket.objects.with_prefix(:prefix => base)
        puts files.collect {|x| x.key} if verbose?

        files = files.sort { |x,y| x.key <=> y.key }

        cleanup_with_limit(files, keep) do |f|
          puts "removing s3 file #{bucket}:#{f}" if dry_run? || verbose?
          f.delete unless dry_run? || local_only?
        end
      end

      def remote_bucket
        unless @remote_bucket
          s3 = AWS::S3.new(:access_key_id => key, :secret_access_key => secret)
          @remote_bucket = s3.buckets.create(bucket)
        end
        @remote_bucket
      end

      def bucket
        config[:s3, :bucket]
      end

      def key
        config[:s3, :key]
      end

      def secret
        config[:s3, :secret]
      end
    end
  end
end
