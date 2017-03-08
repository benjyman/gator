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

    $retry_time = 3600
	opts.on("-r", "--retry_time TIME", "If an obsid download job failed, wait this long before retrying (seconds). Default: #{$retry_time}") {|o| $retry_time = o}
end.parse!

abort("$MWA_DIR not defined.") unless ENV["MWA_DIR"]
abort("$MYGROUP not defined.") unless ENV["MYGROUP"]
abort("$USER not defined.") unless ENV["USER"]

# Multiple tables can be used in the same database, but for now assume it's always the same one.
table_name = "downloads"
# Maximum number of jobs to have in the queue.
max_queue_length = 30


abort("Database (#{$database}) doesn't exist!") unless File.exists?($database)
db = SQLite3::Database.open $database
db.results_as_hash = true

begin
    # Loop forever, until all obsids have been downloaded.
    loop do
        _, jobs_in_queue, len_queue = get_queue("zeus", ENV["USER"])

        db.execute("select * from #{table_name}") do |r|
            obsid = r["Obsid"]
            status = r["Status"]

            # If this obsid has been downloaded or is queued, advance.
            if status == "downloaded"
                next
            # If "downloading", check if the job is still running.
            elsif status == "downloading"
                jobid = r["JobID"]
                unless jobs_in_queue.include? jobid
                    stdout = File.readlines("getNGASdata-#{obsid}-#{jobid}.out").last
                    # If the download was successful...
                    if stdout =~ /File Transfer Success./
                        # ... update the table to say so.
                        db.execute("UPDATE #{table_name} SET Status = 'downloaded' WHERE Obsid = #{obsid}")
                    # If the download was not successful...
                    else
                        # ... label as 'failed', and a download will be tried again later.
                        db.execute("UPDATE #{table_name} SET Status = 'failed' WHERE Obsid = #{obsid}")
                    end

                    db.execute("UPDATE #{table_name} SET Stdout = '#{stdout}' WHERE Obsid = #{obsid}")
                    db.execute("UPDATE #{table_name} SET LastChecked = '#{Time.now}' WHERE Obsid = #{obsid}")
                    puts "#{obsid}: #{stdout}"
                end
            end

            # We can't do anything further if we're already at the maximum queue length.
            break if len_queue >= max_queue_length

            # If the failed obsid has been waiting for long enough, try downloading again.
            if status == "failed" and (Time.now - Time.parse(r["LastChecked"])) > $retry_time
                jobid = download(obsid, 10)
                puts "Submitted #{obsid} as job #{jobid}"
                db.execute("UPDATE #{table_name} SET Status = 'downloading' WHERE Obsid = #{obsid}")
                db.execute("UPDATE #{table_name} SET JobID = #{jobid} WHERE Obsid = #{obsid}")
                db.execute("UPDATE #{table_name} SET LastChecked = '#{Time.now}' WHERE Obsid = #{obsid}")
                len_queue += 1
            # Untouched obsid - download it.
            elsif status == "unqueued"
                jobid = download(obsid, 3)
                puts "Submitted #{obsid} as job #{jobid}"
                db.execute("UPDATE #{table_name} SET Status = 'downloading' WHERE Obsid = #{obsid}")
                db.execute("UPDATE #{table_name} SET JobID = #{jobid} WHERE Obsid = #{obsid}")
                db.execute("UPDATE #{table_name} SET LastChecked = '#{Time.now}' WHERE Obsid = #{obsid}")
                len_queue += 1
            end
        end

        # Check the status of all obsids - have they all been downloaded? If so, exit.
        num_downloaded = db.execute("select * from #{table_name} where Status = 'downloaded'").length
        num_obsids = db.execute("select * from #{table_name}").length
        if num_downloaded == num_obsids
            puts "\nJobs done."
            exit 
        else
            puts "Number of obsids not yet downloaded: #{num_obsids - num_downloaded}"
        end

        puts "Sleeping...\n\n"
        sleep 60
    end

rescue SQLite3::Exception => e 
    puts "Exception occurred: #{e}"

ensure
    db.close if db
end
