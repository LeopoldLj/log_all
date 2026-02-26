// C:\Quantower\Settings\Scripts\Indicators\log_all\log_all.cs
// log_all - raw connection logger for Quantower Algo
// Version: 1.0.2
//
// Copyright QUANTOWER LLC. © 2017-2023. All rights reserved.

using System;
using System.IO;
using System.Text;
using System.Reflection;
using System.Globalization;
using System.Drawing;
using TradingPlatform.BusinessLayer;

namespace log_all
{
    public sealed class log_all : Indicator
    {
        public new const string Version = "1.0.3";

        // Inputs
        [InputParameter("Enable logging", 0)]
        public bool EnableLogging = true;

        [InputParameter("Log quotes (Symbol.NewQuote)", 1)]
        public bool LogQuotes = true;

        [InputParameter("Flush each line", 2)]
        public bool FlushEachLine = true;

        [InputParameter("Log folder (empty = Documents\\QuantowerLogs)", 3)]
        public string LogFolder = "";

        [InputParameter("Log file prefix", 4)]
        public string LogFilePrefix = "log_all";

        [InputParameter("Include ToString() in dumps", 5)]
        public bool IncludeToString = false;

        [InputParameter("Min trade size (filter)", 6, 0, 1000000, 1, 0)]
        public double MinTradeSize = 0;

        private readonly object _sync = new object();
        private StreamWriter _eventsWriter;
        private StreamWriter _quotesWriter;
        private StreamWriter _ticksWriter;
        private string _symbolName = "UNKNOWN_SYMBOL";
        private string _sessionStamp = "";
        private bool _subscribed;

        public log_all()
            : base()
        {
            Name = $"log_all (v{Version})";
            Description = $"Logs raw Symbol.NewLast / Symbol.NewQuote data (v{Version})";

            AddLineSeries("close", Color.CadetBlue, 1, LineStyle.Solid);
            SeparateWindow = false;
        }

        protected override void OnInit()
        {
            InitializeSession();

            if (EnableLogging)
            {
                OpenWriters();
                Subscribe();
                LogEvent("INIT", "Indicator initialized");
            }
            else
            {
                Unsubscribe();
                CloseWriters();
            }
        }

        protected override void OnUpdate(UpdateArgs args)
        {
            try { SetValue(Close(0)); } catch { /* ignore */ }
        }

        protected override void OnSettingsUpdated()
        {
            Unsubscribe();

            if (EnableLogging)
            {
                InitializeSession();
                CloseWriters();
                OpenWriters();
                Subscribe();
                LogEvent("SETTINGS_UPDATED", "Inputs updated");
            }
            else
            {
                CloseWriters();
            }
        }

        private void InitializeSession()
        {
            string rawSymbol = null;
            try
            {
                if (this.Symbol != null)
                    rawSymbol = this.Symbol.Name;
            }
            catch { /* ignore */ }

            _symbolName = SanitizeFileName(string.IsNullOrWhiteSpace(rawSymbol) ? "UNKNOWN_SYMBOL" : rawSymbol);
            _sessionStamp = DateTime.UtcNow.ToString("yyyyMMdd_HHmmss");
        }

        private void Subscribe()
        {
            if (_subscribed)
                return;

            if (this.Symbol == null)
                return;

            this.Symbol.NewLast += OnNewLast;

            if (LogQuotes)
                this.Symbol.NewQuote += OnNewQuote;

            _subscribed = true;
        }

        private void Unsubscribe()
        {
            if (this.Symbol == null)
            {
                _subscribed = false;
                return;
            }

            try { this.Symbol.NewLast -= OnNewLast; } catch { /* ignore */ }
            try { this.Symbol.NewQuote -= OnNewQuote; } catch { /* ignore */ }

            _subscribed = false;
        }

        private void OpenWriters()
        {
            if (!EnableLogging)
                return;

            string baseFolder = LogFolder;
            if (string.IsNullOrWhiteSpace(baseFolder))
                baseFolder = Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.MyDocuments), "QuantowerLogs");

            Directory.CreateDirectory(baseFolder);

            string prefix = string.IsNullOrWhiteSpace(LogFilePrefix) ? "log_all" : LogFilePrefix;
            prefix = SanitizeFileName(prefix);

            string eventsPath = Path.Combine(baseFolder, $"{prefix}__{_symbolName}__{_sessionStamp}__events.csv");
            string quotesPath = Path.Combine(baseFolder, $"{prefix}__{_symbolName}__{_sessionStamp}__quotes.csv");
            string ticksPath = Path.Combine(baseFolder, $"{prefix}__{_symbolName}__{_sessionStamp}__ticks_simple.csv");

            lock (_sync)
            {
                _eventsWriter = CreateWriter(eventsPath);
                _quotesWriter = CreateWriter(quotesPath);

                _eventsWriter.WriteLine("utc_now,event,details");
                _quotesWriter.WriteLine("utc_now,symbol,quote_dump");

                _ticksWriter = CreateWriter(ticksPath);
                _ticksWriter.WriteLine("utc_now,symbol,price,size,side");
            }
        }

        private StreamWriter CreateWriter(string path)
        {
            var stream = new FileStream(path, FileMode.Create, FileAccess.Write, FileShare.Read);
            var writer = new StreamWriter(stream, Encoding.UTF8);
            writer.AutoFlush = FlushEachLine;
            return writer;
        }

        private void CloseWriters()
        {
            lock (_sync)
            {
                try { _eventsWriter?.Flush(); } catch { /* ignore */ }
                try { _quotesWriter?.Flush(); } catch { /* ignore */ }
                try { _ticksWriter?.Flush(); } catch { /* ignore */ }

                try { _eventsWriter?.Dispose(); } catch { /* ignore */ }
                try { _quotesWriter?.Dispose(); } catch { /* ignore */ }
                try { _ticksWriter?.Dispose(); } catch { /* ignore */ }

                _eventsWriter = null;
                _quotesWriter = null;
                _ticksWriter = null;
            }
        }

        private void LogEvent(string evt, string details)
        {
            if (_eventsWriter == null)
                return;

            DateTime now = DateTime.UtcNow;
            string line = string.Join(",",
                now.ToString("o"),
                EscapeCsv(evt),
                EscapeCsv(details));

            lock (_sync)
            {
                _eventsWriter.WriteLine(line);
                if (!FlushEachLine)
                    _eventsWriter.Flush();
            }
        }

        private void OnNewLast(Symbol symbol, Last last)
        {
            if (!EnableLogging)
                return;

            DateTime now = DateTime.UtcNow;
            string symbolName = symbol != null ? symbol.Name : _symbolName;
            double size = last.Size;
            if (MinTradeSize > 0 && size < MinTradeSize)
                return;

            lock (_sync)
            {
                if (_ticksWriter != null)
                {
                    string side = AggressorToSide(last.AggressorFlag);
                    string line = string.Join(",",
                        now.ToString("o"),
                        EscapeCsv(symbolName),
                        FormatNum(last.Price),
                        FormatNum(size),
                        side);
                    _ticksWriter.WriteLine(line);
                    if (!FlushEachLine)
                        _ticksWriter.Flush();
                }

            }
        }

        private void OnNewQuote(Symbol symbol, Quote quote)
        {
            if (!EnableLogging || !LogQuotes || _quotesWriter == null)
                return;

            DateTime now = DateTime.UtcNow;
            string dump = DumpObject(quote);
            string line = string.Join(",",
                now.ToString("o"),
                EscapeCsv(symbol != null ? symbol.Name : _symbolName),
                EscapeCsv(dump));

            lock (_sync)
            {
                _quotesWriter.WriteLine(line);
                if (!FlushEachLine)
                    _quotesWriter.Flush();
            }
        }

        private string DumpObject(object obj)
        {
            if (obj == null)
                return "null";

            try
            {
                Type t = obj.GetType();
                PropertyInfo[] props = t.GetProperties(BindingFlags.Instance | BindingFlags.Public);
                var sb = new StringBuilder();
                sb.Append(t.FullName);

                for (int i = 0; i < props.Length; i++)
                {
                    PropertyInfo p = props[i];
                    object val = null;
                    try { val = p.GetValue(obj, null); } catch { val = "[unreadable]"; }
                    sb.Append(i == 0 ? " {" : ", ");
                    sb.Append(p.Name);
                    sb.Append("=");
                    sb.Append(val == null ? "null" : val.ToString());
                }

                if (props.Length > 0)
                    sb.Append(" }");

                if (IncludeToString)
                {
                    sb.Append(" | ToString=");
                    sb.Append(obj.ToString());
                }

                return sb.ToString();
            }
            catch (Exception ex)
            {
                return "DumpObjectError: " + ex.GetType().Name + " " + ex.Message;
            }
        }

        private static string FormatNum(double value)
        {
            if (double.IsNaN(value) || double.IsInfinity(value))
                return "NA";
            return value.ToString("G17", CultureInfo.InvariantCulture);
        }

        private static string AggressorToSide(AggressorFlag flag)
        {
            if (flag == AggressorFlag.Buy)
                return "BUY";
            if (flag == AggressorFlag.Sell)
                return "SELL";
            return "UNKNOWN";
        }


        private static string EscapeCsv(string s)
        {
            if (s == null)
                return "";

            string cleaned = s.Replace("\r", " ").Replace("\n", " ");
            bool mustQuote = cleaned.IndexOfAny(new[] { ',', '"', '\n', '\r' }) >= 0;
            if (mustQuote)
            {
                cleaned = cleaned.Replace("\"", "\"\"");
                return "\"" + cleaned + "\"";
            }

            return cleaned;
        }

        private static string SanitizeFileName(string s)
        {
            if (string.IsNullOrWhiteSpace(s))
                return "NA";

            foreach (char ch in Path.GetInvalidFileNameChars())
                s = s.Replace(ch, '_');

            return s;
        }
    }
}
