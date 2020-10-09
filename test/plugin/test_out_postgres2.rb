require 'helper'
require 'pg'

class PostgresOutputTest < Test::Unit::TestCase
  def setup
    Fluent::Test.setup
  end

  CONFIG = %[
host database.local
database foo
username bar
password mogera
key_names field1,field2,field3
sql INSERT INTO baz (col1,col2,col3,col4) VALUES (?,?,?,?)
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

  def test_configure_fails_if_both_cols_and_sql_specified
    assert_raise(Fluent::ConfigError) {
      create_driver %[
host database.local
database foo
username bar
password mogera
key_names field1,field2,field3
sql INSERT INTO baz (col1,col2,col3,col4) VALUES (?,?,?,?)
columns col1,col2,col3,col4
      ]
    }
  end

  def test_configure_fails_if_neither_cols_or_sql_specified
    assert_raise(Fluent::ConfigError) {
      create_driver %[
host database.local
database foo
username bar
password mogera
key_names field1,field2,field3
      ]
    }
  end

  def test_key_names_with_spaces
    d = create_driver %[
host database.local
database foo
username bar
password mogera
table baz
key_names time, tag, field1, field2, field3, field4
sql INSERT INTO baz (coltime,coltag,col1,col2,col3,col4) VALUES (?,?,?,?,?,?)
    ]
    assert_equal ["time", "tag", "field1", "field2", "field3", "field4"], d.instance.key_names
  end

  def test_time_and_tag_key
    d = create_driver %[
host database.local
database foo
username bar
password mogera
include_time_key yes
utc
include_tag_key yes
table baz
key_names time,tag,field1,field2,field3,field4
sql INSERT INTO baz (coltime,coltag,col1,col2,col3,col4) VALUES (?,?,?,?,?,?)
    ]
    assert_equal 'INSERT INTO baz (coltime,coltag,col1,col2,col3,col4) VALUES (?,?,?,?,?,?)', d.instance.sql

    time = event_time('2012-12-17 01:23:45 UTC')
    record = {'field1'=>'value1','field2'=>'value2','field3'=>'value3','field4'=>'value4'}
    d.run(default_tag: 'test') do
      d.feed(time, record)
    end
    assert_equal ['test', time, ['2012-12-17T01:23:45Z','test','value1','value2','value3','value4']].to_msgpack, d.formatted[0]
  end

  def test_time_and_tag_key_complex
    d = create_driver %[
host database.local
database foo
username bar
password mogera
include_time_key yes
utc
time_format %Y%m%d-%H%M%S
time_key timekey
include_tag_key yes
tag_key tagkey
table baz
key_names timekey,tagkey,field1,field2,field3,field4
sql INSERT INTO baz (coltime,coltag,col1,col2,col3,col4) VALUES (?,?,?,?,?,?)
    ]
    assert_equal 'INSERT INTO baz (coltime,coltag,col1,col2,col3,col4) VALUES (?,?,?,?,?,?)', d.instance.sql

    time = event_time('2012-12-17 09:23:45 +0900')
    record = {'field1'=>'value1','field2'=>'value2','field3'=>'value3','field4'=>'value4'}
    d.run(default_tag: 'test') do
      d.feed(time, record)
    end
    assert_equal ['test', time, ['20121217-002345','test','value1','value2','value3','value4']].to_msgpack, d.formatted[0]
  end
end
