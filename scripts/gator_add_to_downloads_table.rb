#!/usr/bin/env ruby

$LOAD_PATH.unshift "#{File.dirname(__FILE__)}/.."
require "gator"
require "sqlite3"
require "optparse"

OptionParser.new do |opts|
    opts.banner = "Usage: #{__FILE__} [-d /path/to/database]\nAdd obsids to a database for downloading."
    opts.on("-h", "--help", "Display this message.") {puts opts; exit}

    $database = "#{ENV["MYGROUP"]}/obsids.sqlite"
	opts.on("-d", "--database DATABASE", "Specify the database to be used. By default, this is #{$database}") {|o| $database = o}
end.parse!

abort("$MYGROUP not defined.") unless ENV["MYGROUP"]
abort("$USER not defined.") unless ENV["USER"]

# Multiple tables can be used in the same database, but for now assume it's always the same one.
table_name = "downloads"


if File.exists?($database)
    db = SQLite3::Database.open $database
else
    db = SQLite3::Database.new $database
    db.execute "CREATE TABLE #{table_name}(Obsid INTEGER PRIMARY KEY, Ready INTEGER, Status TEXT, LastChecked TEXT, JobID INTEGER, Stdout TEXT)"
end
db.results_as_hash = true

# Add specified obsids.
# Some intelligent code here could determine if the obsid has already been downloaded.
# Hash?
obsids_in_table = db.execute("select * from #{table_name}").map {|r| r[0]}
obtain_obsids(ARGV).each do |o|
    if obsids_in_table.include? o
        puts "Already in database: #{o}"
        next
    end

    obsids_in_table.push o
    puts "Added: #{o}"
    db.execute "INSERT INTO #{table_name} VALUES(#{to_be_inserted}, 0, 'unqueued', '#{Time.now}', 0, ' ')"
end
