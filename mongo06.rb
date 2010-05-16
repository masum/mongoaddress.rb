# -*- coding: utf-8 -*-
require 'rubygems'
require 'mongo'
require 'csv'
require 'nkf'
require 'kconv'
$KCODE = 'UTF8'

$dbname = "mydb"
$collectionname = "address2"
$db = Mongo::Connection.new("localhost").db($dbname)
$coll = $db.collection($collectionname)
$coll.remove

$csvname = "KEN_ALL.CSV"
# $csvname = "22SHIZUO.CSV"
$bulk = []

def addBulk(k,l,v)
  data = {'key'=>k,'value'=>l,'display'=>v}
  $bulk << data
  if $bulk.length > 1000 then
    $coll.insert($bulk)
    $bulk.clear
  end
end


def addFunc_allPattern(k,l,v)
  ks=k.split(//)
  for i in 0..(ks.length-1)
    addBulk(ks[0..i].join(),l,v)
  end
end

# 最後の１字は登録しない版
def addFunc_lastUnregist(k,l,v)
  ks=k.split(//)
  for i in 0..(ks.length-2)
    addBulk(ks[0..i].join(),l,v)
  end
end

# 頭に県名などをつける版
def addFunc_addPrefix(base,k,l,v)
  ks=k.split(//)
  for i in 0..(ks.length-1)
    addBulk(base+ks[0..i].join(),l,v)
  end
end

# 頭に県名などをつけ、最後の１字を登録しない版
def addFunc_addPrefix_lastUnregist(base,k,l,v)
  ks=k.split(//)
  for i in 0..(ks.length-2)
    addBulk(base+ks[0..i].join(),l,v)
  end
end

# そのまま登録版
def addFunc_direct(k,l,v) 
  addBulk(k,l,v)
end


$total = 0
$ken = nil
$shi = nil
CSV.open($csvname,"r") do |row|
  $total += 1
  adr = {}
  adr['y_ken'] = NKF.nkf('-Sw -Lu -h', row[3])
  adr['y_shi'] = NKF.nkf('-Sw -Lu -h', row[4])
  adr['y_chiku'] = NKF.nkf('-Sw -Lu -h', row[5])
  adr['y_chiku'] = adr['y_chiku'].split('(')[0]
  adr['h_ken'] = row[6].toutf8
  adr['h_shi'] = row[7].toutf8
  adr['h_chiku'] = row[8].toutf8
  next unless adr['h_chiku'].scan(/以下に/).length==0 

  puts $total if(($total % 10000)==0)

  # 地区名の処理
  addFunc_direct(adr['h_ken']+adr['h_shi'], adr['h_chiku'], adr['h_ken']+adr['h_shi']+adr['h_chiku'])
  addFunc_addPrefix(adr['h_ken']+adr['h_shi'], adr['y_chiku'], adr['h_chiku'], adr['h_ken']+adr['h_shi']+adr['h_chiku'])
  addFunc_addPrefix_lastUnregist(adr['h_ken']+adr['h_shi'], adr['h_chiku'], adr['h_chiku'], adr['h_ken']+adr['h_shi']+adr['h_chiku'])

  if ($shi != adr['h_shi']) then # 新しい市に変わった
    $shi = adr['h_shi']
    addFunc_direct(adr['h_ken'], adr['h_shi'], adr['h_ken']+adr['h_shi'])
    addFunc_addPrefix(adr['h_ken'], adr['y_shi'], adr['h_shi'], adr['h_ken']+adr['h_shi'])
    addFunc_addPrefix_lastUnregist(adr['h_ken'], adr['h_shi'], adr['h_shi'], adr['h_ken']+adr['h_shi'])
  end

  if ($ken != adr['h_ken']) then # 新しい県に変わった
    $ken = adr['h_ken']
    addFunc_direct("*", adr['h_ken'], adr['h_ken'])
    addFunc_allPattern(adr['y_ken'], adr['h_ken'], adr['h_ken'])
    addFunc_lastUnregist(adr['h_ken'], adr['h_ken'], adr['h_ken'])
  end

end
if $bulk.length > 0 then
  $coll.insert($bulk)
end
