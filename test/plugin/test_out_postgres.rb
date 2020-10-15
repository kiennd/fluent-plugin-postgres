require 'helper'
require 'pg'
require 'logger'

class PostgresOutputTest < Test::Unit::TestCase
  def setup
    Fluent::Test.setup
  end
  $logger = Logger.new(STDOUT)
  
  CONFIG = %[
    host 172.16.24.224
    database postgres
    username postgres
    password 1
    include_time_key yes
    utc
    time_format %Y%m%d-%H%M%S
    time_key timekey
    include_tag_key yes
    tag_key tagkey
    table baz
    key_names timekey,tagkey,field1
    sql INSERT INTO baz (coltime,coltag,col1) VALUES (?,?,?)
  ]

  def create_driver(conf=CONFIG)
    d = Fluent::Test::Driver::Output.new(Fluent::Plugin::PostgresOutput).configure(conf)
    d.instance.instance_eval {
      def client
        obj = Object.new
        obj.instance_eval {
          def prepare(*args); true; end
        def exec_prepared(*args); true; end
        def close; true; end
        }
        obj
      end
    }
    d
  end

  def test_time_and_tag_key_complex
    d = create_driver %[
      host 172.16.24.28
      database postgres
      username postgres
      password 1
      include_time_key yes
      utc
      time_format %Y%m%d-%H%M%S
      time_key timekey
      include_tag_key yes
      tag_key tagkey
      json_fields field1,field2
      key_names timekey,tagkey,field1,field2
      hash_input_fields_index 1,3
      sql INSERT INTO baz (coltime,coltag,col1,col2,md5) VALUES ($1,$2,$3,$4,$5)
    ]

    time = event_time('2012-12-17 09:23:45 +0900')
    inp = '{"field1":{"1":2},"field2":[{"a":"b","c":"d"}]}'
    
    record = JSON.parse(inp)
    $logger.info(record);
    d.run(default_tag: 'test') do
      d.feed(time, record)
    end
  end
end
