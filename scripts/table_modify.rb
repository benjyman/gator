#!/usr/bin/env ruby

$LOAD_PATH.unshift "#{File.dirname(__FILE__)}/.."
require "gator"
require "sqlite3"

database = ARGV[0]
table_name = "downloads"

db = SQLite3::Database.open database
db.results_as_hash = true

db.execute("select * from #{table_name}") do |r|
    obsid = r["Obsid"]
    status = r["Status"]
    if status == "downloading"
        print "#{obsid} "
        db.execute("UPDATE #{table_name} SET Status = 'unqueued' WHERE Obsid = #{obsid}")
    end
end
