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
    opts.banner = "Usage: #{__FILE__} [-d /path/to/database]\nPurge failed (default) or all RTS runs from an SQLite table."
    opts.on("-h", "--help", "Display this message.") {puts opts; exit}

    $database = "#{ENV["MYGROUP"]}/rts.sqlite"
	opts.on("-d", "--database DATABASE", "Specify the database to be used. Default: #{$database}") {|o| $database = o}

    $purge_all = false
	opts.on("-a", "--all", "Purge all rows of the table, even if they've been peeled.") {|o| $purge_all = true}
end.parse!


table_name = "downloads"


db = SQLite3::Database.open $database
db.results_as_hash = true

if not $purge_all
    rows = db.execute("SELECT * FROM #{table_name}
                       WHERE Status IS NOT 'peeled'
                       AND   Status IS NOT 'unqueued'")
    print "Will delete #{rows.length} failed RTS runs. Proceed? [yN] "
else
    rows = db.execute("SELECT * FROM #{table_name}")
    print "Will delete #{rows.length} RTS runs. Proceed? [yN] "
end

response = STDIN.gets.chomp
abort("*** Aborting.") unless response.downcase == "y"

rows.each do |r|
    obsid = r["Obsid"]
    path = r["Path"]

    FileUtils.rm_r path unless path.strip.empty?
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
