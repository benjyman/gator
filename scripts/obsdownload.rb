#!/usr/bin/env ruby

$LOAD_PATH.unshift "#{File.dirname(__FILE__)}/.."
require "gator"


if __FILE__ == $0
    abort "No obsids supplied!" if ARGV.length == 0

    obtain_obsids(ARGV).each do |o|
        obj = Obsid.new(o)
        obj.download
        puts obj.download_jobid
    end
end
