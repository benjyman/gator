#!/usr/bin/env ruby

$LOAD_PATH.unshift "#{File.dirname(__FILE__)}/.."
require "gator"
require "sqlite3"

database = ARGV[0]
table_name = "downloads"

bad_obsids = File.readlines(ARGV[1]).map { |o| o.strip.to_i }

db = SQLite3::Database.open database
db.results_as_hash = true

db.execute("select * from #{table_name}") do |r|
    obsid = r["Obsid"]
    if bad_obsids.include? obsid
        print "#{obsid} "
        db.execute("DELETE FROM #{table_name} WHERE Obsid = #{obsid}")
    end
end
