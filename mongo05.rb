# -*- coding: utf-8 -*-
require 'rubygems'
require 'mongo'
require 'csv'
require 'nkf'
require 'kconv'
$KCODE = 'UTF8'

$dbname = "mydb"
$collectionname = "address"
$db = Mongo::Connection.new("localhost").db($dbname)
$coll = $db.collection($collectionname)
$coll.remove

$csvname = "KEN_ALL.CSV"
$bulk = []
6.times {|i|
  $bulk[i] = []
}
$total = 0
$adr1 = nil
$adr2 = nil
CSV.open($csvname,"r") do |row|
  $total += 1
  adr = {}
  adr['key1'] = NKF.nkf('-Sw -Lu -h', row[3])
  adr['key2'] = NKF.nkf('-Sw -Lu -h', row[4])
  adr['key3'] = NKF.nkf('-Sw -Lu -h', row[5])
  adr['key3'] = adr['key3'].split('(')[0]
  adr['value1'] = row[6].toutf8
  adr['value2'] = row[7].toutf8
  adr['value3'] = row[8].toutf8
  next unless adr['value3'].scan(/以下に/).length==0 
  puts $total if(($total % 10000)==0)

  $bulk[5] << {'key'=>adr['value1']+adr['value2']+adr['key3'],'value'=>adr['value3']}

  if ($adr2 != adr['value2']) then
    $adr2 = adr['value2']
    $bulk[1] << {'key'=>adr['value1'],'value'=>adr['value2']}
    $bulk[3] << {'key'=>adr['value1']+adr['value2'],'value'=>adr['value3']}
    $bulk[4] << {'key'=>adr['value1']+adr['key2'],'value'=>adr['value2']}
    if($bulk[5].length > 0) then
      $coll.insert($bulk[5])
      $bulk[5] = []
    end
  end
  if ($adr1 != adr['value1']) then
    if ($adr1 != nil) then
      $coll.insert($bulk[1])
      $coll.insert($bulk[3])
      $coll.insert($bulk[4])
      $bulk[1] = []
      $bulk[3] = []
      $bulk[4] = []
    end
    $adr1 = adr['value1']
    puts $adr1
    $bulk[0] << {'key'=>'*','value'=>adr['value1']}
    $bulk[2] << {'key'=>adr['key1'],'value'=>adr['value1']}
  end
end
$coll.insert($bulk[0])
$coll.insert($bulk[2])

$coll.find().each { |doc| puts doc.inspect }
puts $coll.count()
