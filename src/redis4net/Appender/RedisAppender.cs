using System;
using log4net.Appender;
using log4net.Core;
using log4net.Util;

namespace redis4net.Appender
{
    using redis4net.Redis;

    public class RedisAppender : AppenderSkeleton
    {
        private DateTime? errorStateDateTime;
        protected ConnectionFactory ConnectionFactory { get; set; }
        public string RemoteAddress { get; set; }
        public int RemotePort { get; set; }
        public string ListName { get; set; }

        public int SecondsBeforeOpeningConnectionAfterFailed { get; set; } = 0;
        public int FailedConnectionRetryTimeoutInSeconds { get; set; } = 1;

        public override void ActivateOptions()
        {
            base.ActivateOptions();
            this.ErrorHandler = new LogAllErrorHandler(new OnlyOnceErrorHandler());

            InitializeConnectionFactory();

            ErrorHandler.Error("!!!!!!Calling ActivateOptions!!!!!");
        }


        protected virtual void InitializeConnectionFactory()
        {
            var connection = new Connection();
            ConnectionFactory = new ConnectionFactory(connection, RemoteAddress, RemotePort, FailedConnectionRetryTimeoutInSeconds, ListName);
        }

        protected override void Append(log4net.Core.LoggingEvent loggingEvent)
        {
            if (errorStateDateTime.HasValue)
            {
                if (errorStateDateTime.Value.AddSeconds(SecondsBeforeOpeningConnectionAfterFailed) < DateTime.UtcNow)
                {
                    errorStateDateTime = null;
                    AppendCore(loggingEvent);
                }
                else
                {
                    ErrorHandler.Error("Unable to send logging event to remote redis host " + RemoteAddress + " on port " + RemotePort + " because the connection couldnt be opened");
                }
            }
            else
            {
                AppendCore(loggingEvent);
            }
        }

        private void AppendCore(LoggingEvent loggingEvent)
        {
            try
            {
                var message = RenderLoggingEvent(loggingEvent);
                var connection = ConnectionFactory.GetConnection();
                if (connection != null)
                {
                    connection.AddToList(message);
                }
                else
                {
                    this.errorStateDateTime = DateTime.UtcNow;
                }
            }
            catch (Exception exception)
            {
                ErrorHandler.Error("Unable to send logging event to remote redis host " + RemoteAddress + " on port " + RemotePort, exception, ErrorCode.WriteFailure);
                this.errorStateDateTime = DateTime.UtcNow;
            }
        }
    }

    public class LogAllErrorHandler : IErrorHandler
    {
        private readonly OnlyOnceErrorHandler onlyOnceErrorHandler;

        public LogAllErrorHandler(OnlyOnceErrorHandler onlyOnceErrorHandler)
        {
            this.onlyOnceErrorHandler = onlyOnceErrorHandler;
        }

        public void Error(string message, Exception e, ErrorCode errorCode)
        {
            onlyOnceErrorHandler.Error(message, e, errorCode);
            onlyOnceErrorHandler.Reset();
        }

        public void Error(string message, Exception e)
        {
            onlyOnceErrorHandler.Error(message, e);
            onlyOnceErrorHandler.Reset();
        }

        public void Error(string message)
        {
            onlyOnceErrorHandler.Error(message);
            onlyOnceErrorHandler.Reset();
        }
    }
}
