require 'fluent/plugin/output'
require 'pg'
require 'logger'
require 'digest'

class Fluent::Plugin::PostgresOutput < Fluent::Plugin::Output
  Fluent::Plugin.register_output('postgres', self)

  helpers :inject, :compat_parameters

  config_param :host, :string
  config_param :port, :integer, :default => nil
  config_param :database, :string
  config_param :username, :string
  config_param :password, :string, :default => ''

  config_param :key_names, :string, :default => nil # nil allowed for json format
  config_param :sql, :string, :default => nil
  config_param :table, :string, :default => nil
  config_param :columns, :string, :default => nil
  config_param :json_fields, :string, :default => nil
  config_param :format, :string, :default => "raw" # or json
  config_param :hash_input_fields_index, :string, :default => nil
  config_param :update_key_index, :string, :default => nil
  config_param :update, :string, :default => nil
  
  attr_accessor :handler



  $logger = Logger.new(STDOUT)
  # We don't currently support mysql's analogous json format
  def configure(conf)
    #$logger.info("-----------------------------------configure------------------------------");
    compat_parameters_convert(conf, :inject)
    super

    if @format == 'json'
      @format_proc = Proc.new{|tag, time, record| record.to_json}
    else
      if !@hash_input_fields_index.nil? 
        @hash_input_fields_index = @hash_input_fields_index.split(/\s*,\s*/)
      end

      @key_names = @key_names.split(/\s*,\s*/)
      if !@json_fields.nil? 
        @json_fields = @json_fields.split(/\s*,\s*/)
      end
      @format_proc = Proc.new{|tag, time, record| @key_names.map{ |k|
        if !@json_fields.nil? && (@json_fields.include? k)
          record[k].to_json
        else
          record[k]
        end
      }}
    end

    if @columns.nil? and @sql.nil?
      raise Fluent::ConfigError, "columns or sql MUST be specified, but missing"
    end
    if @columns and @sql
      raise Fluent::ConfigError, "both of columns and sql are specified, but specify one of them"
    end
  end

  def start
    super
  end

  def shutdown
    super
  end

  def format(tag, time, record)
    #$logger.info("-----------------------------------format------------------------------");
    record = inject_values_to_record(tag, time, record)
    [tag, time, @format_proc.call(tag, time, record)].to_msgpack
  end

  def multi_workers_ready?
    true
  end

  def formatted_to_msgpack_binary?
    true
  end

  def client2
    #$logger.info("-----------------------------------client2------------------------------");
    PG::Connection.new({
      :host => @host, :port => @port,
      :user => @username, :password => @password,
      :dbname => @database
    })
  end

  def write(chunk)
    #$logger.info('-----------------------------------write-------------------------------------')
    #$logger.info(@hash_input_fields_index);

    handler = self.client2()
    handler.prepare("write", @sql)
    if !@update_key_index.nil?
      handler.prepare("update", @update)
    end
    chunk.msgpack_each { |tag, time, data|
      if !@hash_input_fields_index.nil?
        hashInput = ''
        @hash_input_fields_index.each { |item|
          #$logger.info(data[item.to_i])
          hashInput = hashInput + data[item.to_i]
        }
        md5 = Digest::MD5.new 
        md5.reset
        md5 << hashInput
        data.append(md5.hexdigest)
      end
      begin
        handler.exec_prepared("write", data)
      rescue PG::UniqueViolation => uniqE
        $logger.info('-----------------------------------UniqueViolation-------------------------------------')
        begin
          $logger.info('-----------------------------------Update-------------------------------------')
          if !@update_key_index.nil?
            data.append(data[update_key_index.to_i])
          end
          handler.exec_prepared("update", data)
        rescue => e
          $logger.info('-----------------------------------Update Error-------------------------------------')
        end
      end
    }
    handler.close
  end
end
