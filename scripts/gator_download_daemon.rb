#!/usr/bin/env ruby

$LOAD_PATH.unshift "#{File.dirname(__FILE__)}/.."
require "gator"
require "sqlite3"
require "optparse"

OptionParser.new do |opts|
    opts.banner = "Usage: #{__FILE__} [-d /path/to/database]\nDownload all of the obsids specified in the database."
    opts.on("-h", "--help", "Display this message.") {puts opts; exit}

    $database = "#{ENV["MYGROUP"]}/obsids.sqlite"
	opts.on("-d", "--database DATABASE", "Specify the database to be used. Default: #{$database}") {|o| $database = o}

    $retry_time = 1800
	opts.on("-r", "--retry_time TIME", "If an obsid download job failed, wait this long before retrying (seconds). Default: #{$retry_time}") {|o| $retry_time = o.to_i}

    $sleep_time = 60
	opts.on("-s", "--sleep_time TIME", "Amount of time to wait before re-analysing the queue (seconds). Default: #{$sleep_time}") {|o| $sleep_time = o.to_i}
end.parse!

abort("$MWA_DIR not defined.") unless ENV["MWA_DIR"]
abort("$MYGROUP not defined.") unless ENV["MYGROUP"]
abort("$USER not defined.") unless ENV["USER"]

# Multiple tables can be used in the same database, but for now assume it's always the same one.
table_name = "downloads"
# Maximum number of jobs to have in the queue.
max_queue_length = 16


abort("Database (#{$database}) doesn't exist!") unless File.exists?($database)
db = SQLite3::Database.open $database
db.results_as_hash = true

begin
    # Loop forever, until all obsids have been downloaded.
    loop do
        _, jobs_in_queue, len_queue = get_queue("zeus", ENV["USER"])

        db.execute("SELECT * FROM #{table_name}") do |r|
            obsid = r["Obsid"]
            status = r["Status"]

            # If this obsid has been downloaded or is queued, advance.
            if status == "downloaded"
                next
            # If "downloading", check if the job is still running.
            elsif status == "downloading"
                jobid = r["JobID"]
                unless jobs_in_queue.include? jobid
                    stdout = File.read("#{ENV["MWA_DIR"].chomp('/')}/data/#{obsid}/getNGASdata-#{obsid}-#{jobid}.out")
                    # If the download was successful...
                    if stdout.scan(/File Transfer Success/).count == 1
                        # ... update the table to say so.
                        status = "downloaded"
                    # If the download was not successful...
                    else
                        # ... label as 'failed', and a download will be tried again later.
                        status = "failed"
                    end

                    last_line = stdout.split("\n").last
                    puts "#{obsid}: #{last_line}"
                    db.execute("UPDATE #{table_name}
                                SET LastChecked = '#{Time.now}',
                                    Stdout = '#{last_line.gsub('\'', "")}',
                                    Status = '#{status}'
                                WHERE Obsid = #{obsid}")
                end
            end
        end

        db.execute("SELECT * FROM #{table_name}") do |r|
            # We can't do anything further if we're already at the maximum queue length.
            break if len_queue >= max_queue_length

            obsid = r["Obsid"]
            status = r["Status"]

            # If the obsid is unqueued, download it.
            # If the failed obsid has been waiting for long enough, try downloading again.
            if status == "unqueued" or (status == "failed" and (Time.now - Time.parse(r["LastChecked"])) > $retry_time)
                obj = Obsid.new(obsid)
                obj.download
                jobid = obj.download_jobid
                puts "Submitted #{obsid} as job #{jobid}"
                db.execute("UPDATE #{table_name}
                            SET Status = 'downloading',
                                JobID = #{jobid},
                                LastChecked = '#{Time.now}'
                            WHERE Obsid = #{obsid}")
                len_queue += 1
            end

        end

        # Check the status of all obsids - have they all been downloaded? If so, exit.
        num_not_downloaded = db.execute("SELECT * FROM #{table_name} WHERE NOT Status = 'downloaded'").length
        if num_not_downloaded == 0
            puts "\nJobs done."
            exit
        else
            puts "Number of obsids not yet downloaded: #{num_not_downloaded}"
        end

        puts "Sleeping...\n\n"
        sleep $sleep_time
    end

rescue SQLite3::Exception => e
    puts "#{e.class}: #{e}"

ensure
    db.close if db
end
