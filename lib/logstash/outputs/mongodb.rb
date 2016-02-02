# encoding: utf-8
require "logstash/outputs/base"
require "logstash/namespace"
require "mongo"
require_relative "bson/big_decimal"
require_relative "bson/logstash_timestamp"
# require_relative "bson/logstash_event"

class LogStash::Outputs::Mongodb < LogStash::Outputs::Base

  config_name "mongodb"

  # a MongoDB URI to connect to
  # See http://docs.mongodb.org/manual/reference/connection-string/
  config :uri, :validate => :string, :required => true

  # The database to use
  config :database, :validate => :string, :required => true

  # The collection to use. This value can use `%{foo}` values to dynamically
  # select a collection based on data in the event.
  config :collection, :validate => :string, :required => true

  # If true, store the @timestamp field in mongodb as an ISODate type instead
  # of an ISO8601 string.  For more information about this, see
  # http://www.mongodb.org/display/DOCS/Dates
  config :isodate, :validate => :boolean, :default => false

  # Number of seconds to wait after failure before retrying
  config :retry_delay, :validate => :number, :default => 3, :required => false

  # If true, an "_id" field will be added to the document before insertion.
  # The "_id" field will use the timestamp of the event and overwrite an existing
  # "_id" field in the event.
  config :generateId, :validate => :boolean, :default => false

  # The hash provided here will be passed to mongo as a query, the first matching
  # document will be updated with the given event. If no document has been 
  # matched a new document will be inserted.
  # The values of the hash can use `%{foo}` values to dynamically populate the 
  # query.
  config :upsert, :validate => :hash, :required => false

  def check_config_validity
    if @upsert and @generateId
      raise LogStash::ConfigurationError, "You cannot enable both generateId and make use of upsert."
    end
  end

  public
  def register
    Mongo::Logger.logger = @logger
    conn = Mongo::Client.new(@uri)
    @db = conn.use(@database)

    check_config_validity
  end # def register

  def receive(event)
    begin
      # Our timestamp object now has a to_bson method, using it here
      # {}.merge(other) so we don't taint the event hash innards
      document = {}.merge(event.to_hash)
      if !@isodate
        # not using timestamp.to_bson
        document["@timestamp"] = event["@timestamp"].to_json
      end
      if @generateId
        document['_id'] = BSON::ObjectId.from_time(event["@timestamp"])
      end
      if not @upsert
        @db[event.sprintf(@collection)].insert_one(document)
      else
        query = Hash[@upsert.map { |k,v| [k, event.sprintf(v)] }]
        @db[event.sprintf(@collection)]\
          .find(query)\
          .update_one({ '$set' => document }, { :upsert => true })
      end
     rescue => e
      @logger.warn("Failed to send event to MongoDB", :event => event, :exception => e,
                   :backtrace => e.backtrace)
      if e.message =~ /^E11000/
          # On a duplicate key error, skip the insert.
          # We could check if the duplicate key err is the _id key
          # and generate a new primary key.
          # If the duplicate key error is on another field, we have no way
          # to fix the issue.
      else
        sleep @retry_delay
        retry
      end
    end
  end # def receive
end # class LogStash::Outputs::Mongodb
