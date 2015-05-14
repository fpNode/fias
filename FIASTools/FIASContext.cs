using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;

namespace fpNode.FIAS
{
    using fpNode.FIAS.Models;
    using System;
    using System.Data.Entity;
    using System.Data.Entity.ModelConfiguration.Conventions;
    using System.Linq;

    public class FIASContext : DbContext
    {
        // Your context has been configured to use a 'Context' connection string from your application's 
        // configuration file (App.config or Web.config). By default, this connection string targets the 
        // 'fpNode.FIAS.Context' database on your LocalDb instance. 
        // 
        // If you wish to target a different database and/or database provider, modify the 'Context' 
        // connection string in the application configuration file.
        public FIASContext(string connStr)
            : base(connStr)
        {
        }

        // Add a DbSet for each entity type that you want to include in your model. For more information 
        // on configuring and using a Code First model, see http://go.microsoft.com/fwlink/?LinkId=390109.

        public virtual DbSet<AddressObjectsObject> AddressObjects { get; set; }
        public virtual DbSet<HousesHouse> Houses { get; set; }
        public virtual DbSet<HouseIntervalsHouseInterval> HouseIntervals { get; set; }
        public virtual DbSet<LandmarksLandmark> Landmarks { get; set; }
        public virtual DbSet<AddressObjectTypesAddressObjectType> AddressObjectTypes { get; set; }

        protected override void OnModelCreating(DbModelBuilder modelBuilder)
        {
            base.OnModelCreating(modelBuilder);
            modelBuilder.Conventions.Remove<PluralizingTableNameConvention>();
        }
    }
}

namespace fpNode.FIAS.Models
{
    [Table("ADDROBJ")] 
    public partial class AddressObjectsObject
    {

    }

    [MetadataType(typeof(AddressObjectsObjectMetaData))]
    public partial class AddressObjectsObject
    {
        public class AddressObjectsObjectMetaData
        {
            [Key]
             public object AOID;
        }
    }

    [Table("HOUSE")]
    public partial class HousesHouse
    {
    }

    [MetadataType(typeof(HousesHouseMetaData))]
    public partial class HousesHouse
    {
        public class HousesHouseMetaData
        {
            [Key]
            public object HOUSEID;
        }
    }

    [Table("HOUSEINT")]
    public partial class HouseIntervalsHouseInterval
    {
    }

    [MetadataType(typeof(HouseIntervalsHouseIntervalMetaData))]
    public partial class HouseIntervalsHouseInterval
    {
        public class HouseIntervalsHouseIntervalMetaData
        {
            [Key]
             public object HOUSEINTID;
        }
    }


    [Table("LANDMARK")]
    public partial class LandmarksLandmark
    {
    }

    [MetadataType(typeof(LandmarksLandmarkMetaData))]
    public partial class LandmarksLandmark
    {
        public class LandmarksLandmarkMetaData
        {
            [Key]
            public object LANDID;
        }
    }

    [Table("SOCRBASE")]
    public partial class AddressObjectTypesAddressObjectType
    {
    }

    [MetadataType(typeof(AddressObjectTypesAddressObjectTypeMetaData))]
    public partial class AddressObjectTypesAddressObjectType
    {
        public class AddressObjectTypesAddressObjectTypeMetaData
        {
            [Key]
            public object KOD_T_ST;
        }
    }
}