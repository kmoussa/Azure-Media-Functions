//------------------------------------------------------------------------------
// <auto-generated>
//     This code was generated from a template.
//
//     Manual changes to this file may cause unexpected behavior in your application.
//     Manual changes to this file will be overwritten if the code is regenerated.
// </auto-generated>
//------------------------------------------------------------------------------

namespace shahidtelemetryModel
{
    using System;
    using System.Collections.Generic;
    
    public partial class StreamingData
    {
        public int Id { get; set; }
        public string PartitionKey { get; set; }
        public string RowKey { get; set; }
        public Nullable<System.DateTimeOffset> TimeStamp { get; set; }
        public string Type { get; set; }
        public string Name { get; set; }
        public Nullable<System.DateTime> ObservationTime { get; set; }
        public Nullable<System.Guid> ServiceId { get; set; }
        public string HostName { get; set; }
        public string StatusCode { get; set; }
        public string ResultCode { get; set; }
        public Nullable<int> RequestCount { get; set; }
        public Nullable<long> BytesSent { get; set; }
        public Nullable<long> ServerLatency { get; set; }
        public Nullable<long> E2ELatency { get; set; }
    }
}