#!/usr/bin/env ruby

require "fileutils"
require "sqlite3"
$LOAD_PATH.unshift "#{File.dirname(__FILE__)}/.."
require "gator"

database = "/group/mwaeor/cjordan/rts.sqlite"
table_name = "downloads"

abort("$MWA_DIR not defined.") unless ENV["MWA_DIR"]
$mwa_dir = ENV["MWA_DIR"].chomp('/')

db = SQLite3::Database.open database
db.results_as_hash = true

db.execute("select * from #{table_name}") do |r|
    obsid = r["Obsid"]
    status = r["Status"]

    to_be_deleted = []
    path = "#{$mwa_dir}/data/#{obsid}"
    if status != "peeled" and status != "unqueued"
        to_be_deleted.push Dir.glob("#{path}/rts_*")
        to_be_deleted.push Dir.glob("#{path}/RTS-*")
        to_be_deleted.push Dir.glob("#{path}/uvdump_*")
        to_be_deleted.push Dir.glob("#{path}/*.mwaf")
        to_be_deleted.push Dir.glob("#{path}/BandpassCalibration_*")
        to_be_deleted.push Dir.glob("#{path}/DI_JonesMatrices_*")
        to_be_deleted.push Dir.glob("#{path}/sRTS_*")
        to_be_deleted.push Dir.glob("#{path}/cj_*")
        to_be_deleted.push Dir.glob("#{path}/flagged_*")
        to_be_deleted.push Dir.glob("#{path}/gpufiles_*")
        to_be_deleted.push Dir.glob("#{path}/obsid.dat")
        to_be_deleted.push Dir.glob("#{path}/srclist_puma*")
        to_be_deleted.push Dir.glob("#{path}/BPcal.png")
        to_be_deleted.flatten!

        FileUtils.rm to_be_deleted
        # Dir.glob("#{path}/*").sort.each do |f|
        #     if to_be_deleted.include? f
        #         next
        #     else
        #         puts f
        #     end
        # end
        # STDIN.gets.chomp
        db.execute("UPDATE #{table_name} SET Status = 'unqueued' WHERE Obsid = #{obsid}")
        puts obsid
    end
end
