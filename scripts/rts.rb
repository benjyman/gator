#!/usr/bin/env ruby

# This script is intended to run the RTS for every obsid specified.
# It assumes that the data for each obsid has already been downloaded.

$LOAD_PATH.unshift "#{File.dirname(__FILE__)}/.."
require "gator"


if __FILE__ == $0
    abort "No obsids supplied!" if ARGV.length == 0

    obtain_obsids(ARGV).each do |o|
        setup_jobid, timestamp = rts_setup(o)
        puts setup_jobid
        puts patch_jobid = rts_patch(o, setup_jobid, timestamp, mins: 40, peel: true)
    end
end
