#!/usr/bin/env ruby

# This script deletes all of the failed RTS runs from a table and re-labels
# the observations as "unqueued", such that running the daemon on the table
# will allow you to have another go at calibration.

require "optparse"
require "fileutils"
require "sqlite3"

$LOAD_PATH.unshift "#{File.dirname(__FILE__)}/.."
require "gator"

OptionParser.new do |opts|
    opts.banner = "Usage: #{__FILE__} [-d /path/to/database]\nDownload all of the obsids specified in the database."
    opts.on("-h", "--help", "Display this message.") {puts opts; exit}

    $database = "#{ENV["MYGROUP"]}/rts.sqlite"
	opts.on("-d", "--database DATABASE", "Specify the database to be used. Default: #{$database}") {|o| $database = o}
end.parse!


table_name = "downloads"


db = SQLite3::Database.open $database
db.results_as_hash = true

db.execute("SELECT * FROM #{table_name}") do |r|
    obsid = r["Obsid"]
    path = r["Path"]
    status = r["Status"]

    if status != "peeled" and status != "unqueued"
        FileUtils.rm_r path
        db.execute("UPDATE #{table_name}
                    SET Path = ' ',
                        Status = 'unqueued',
                        LastChecked = '#{Time.now}',
                        SetupJobID = 0,
                        RTSJobID = 0,
                        Stdout = ' '
                    WHERE Obsid = #{obsid} AND Path = '#{path}'")
        puts "#{obsid} #{path}"
    end
end
