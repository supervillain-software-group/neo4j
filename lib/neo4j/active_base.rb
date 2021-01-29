module Neo4j
  # To contain any base login for ActiveNode/ActiveRel which
  # is external to the main classes
  module ActiveBase
    class << self
      # private?
      def current_session
        (SessionRegistry.current_session ||= establish_session).tap do |session|
          fail 'No session defined!' if session.nil?
        end
      end

      def on_establish_session(&block)
        @establish_session_block = block
      end

      def establish_session
        make_session_wrap!(@establish_session_block.call) if @establish_session_block
      end

      def current_transaction_or_session
        current_transaction || current_session
      end

      def query(*args)
        current_transaction_or_session.query(*args)
      end

      # Should support setting session via config options
      def current_session=(session)
        SessionRegistry.current_session = make_session_wrap!(session)
      end

      def current_adaptor=(adaptor)
        self.current_session = Neo4j::Core::CypherSession.new(adaptor)
      end

      # the "session" parameter of Neo4j::Core::CypherSession::Transactions::Base
      # simply needs to be the same object for all Transactions in the same thread.
      # cheat by having a class that acts as a shared object for all mock transactions
      # it is used two ways by Transactions::Base internally:
      # 1. as a hash key (e.g. a hash that contains all active transactions for the session)
      # 2. it needs to have a "version" for some silly conditional in the code
      class MockSessionRegistry
        extend ActiveSupport::PerThreadRegistry
        attr_accessor :session

        # just like SessionRegistry, ensure each thread gets its own session
        # by using PerThreadRegistry and a lazy-intialized attr
        def self.current_session
          self.session ||= self.new
        end

        # return whatever version the regular HTTP or Bolt session would've returned
        def version
          Neo4j::ActiveBase.current_session.version
        end
      end

      # default to no transaction, unless forced externally
      def run_transaction(run_in_tx = false)
        Neo4j::Transaction.run(current_session, run_in_tx) do |tx|
          if tx
            yield tx
          else
            # some code paths in neo4jrb rely on having a properly-constructed
            # transaction stack that fires its "after_commit" hook at the right
            # time. the easiest way to accomplish this is with an actual transaction
            # object which will construct right the transaction nesting to preserve
            # callback order, but which is not a real HTTP or Bolt transaction
            mock_tx = Neo4j::Core::CypherSession::Transactions::Base.new(MockSessionRegistry.current_session)
            # overwrite unimplemented methods with no-op methods
            def mock_tx.commit; end
            def mock_tx.delete; end
            # pass back the no-op transaction instead of an HTTP or Bolt transaction
            return_value = yield mock_tx
            # call close to finish the callback chain
            mock_tx.close
            # be sure to return the value of the transaction so e.g. `update!` calls
            # still return true or false
            return_value
         end
        end
      end

      def new_transaction
        validate_model_schema!
        Neo4j::Transaction.new(current_session)
      end

      def new_query(options = {})
        validate_model_schema!
        Neo4j::Core::Query.new({session: current_session}.merge(options))
      end

      def magic_query(*args)
        if args.empty? || args.map(&:class) == [Hash]
          ActiveBase.new_query(*args)
        else
          ActiveBase.current_session.query(*args)
        end
      end

      def current_transaction
        validate_model_schema!
        Neo4j::Transaction.current_for(current_session)
      end

      def label_object(label_name)
        Neo4j::Core::Label.new(label_name, current_session)
      end

      def logger
        @logger ||= (Neo4j::Config[:logger] || ActiveSupport::Logger.new(STDOUT))
      end

      private

      def validate_model_schema!
        Neo4j::ModelSchema.validate_model_schema! unless Neo4j::Migrations.currently_running_migrations
      end

      def make_session_wrap!(session)
        session.adaptor.instance_variable_get('@options')[:wrap_level] = :proc
        session
      end
    end
  end
end
