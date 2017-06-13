#!/usr/bin/env ruby

$LOAD_PATH.unshift "#{File.dirname(__FILE__)}/.."
require "gator"
require "sqlite3"

database = "/group/mwaeor/cjordan/rts.sqlite"
table_name = "downloads"


db = SQLite3::Database.open database
db.results_as_hash = true

db.execute("select * from #{table_name}") do |r|
    obsid = r["Obsid"]
    status = r["Status"]
# [1127146736, 1133349792].each do |r|
#     obsid = r

    begin
        # Inspect the STDOUT files.
        rts_stdout_log = Dir.glob("/astro/mwaeor/MWA/data/#{obsid}/RTS*.out").sort_by { |l| File.mtime(l) }.last
        # If it's too big, then it failed.
        if File.stat(rts_stdout_log).size.to_f > 1000000
            status = "failed"
            final = "*** huge stdout - tiles probably need to be flagged."
        # If there's a line about "could not open...", the data isn't there.
        elsif File.read(rts_stdout_log).match(/Could not open array file for reading/)
            status = "no data"
            final = "*** probably no gpubox files available for this obsid."
        elsif File.read(rts_stdout_log).match(/Error: Unable to set weighted average frequency. No unflagged channels/)
            status = "???"
            final = "Error: Unable to set weighted average frequency. No unflagged channels"
        end

        # Skip to the end if we already have a status.
        unless status
            # Read the latest node001.log file.
            node001_log = Dir.glob("/astro/mwaeor/MWA/data/#{obsid}/*node001.log").sort_by { |l| File.mtime(l) }.last
            final = File.readlines(node001_log).last.strip
            if final =~ /LogDone. Closing file/
                # Check the file size. Big enough -> peeled. Too small -> patched.
                if File.stat(node001_log).size > 10000000
                    status = "peeled"
                else
                    status = "patched"
                end
            else
                puts obsid, final
                status = "failed"
            end
        end

        db.execute("UPDATE #{table_name} SET Status = '#{status}' WHERE Obsid = #{obsid}")
        db.execute("UPDATE #{table_name} SET Stdout = '#{final}' WHERE Obsid = #{obsid}")
    rescue TypeError
        db.execute("UPDATE #{table_name} SET Status = 'unqueued' WHERE Obsid = #{obsid}")
        next
    end
end
