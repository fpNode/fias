namespace FIASSplit.Models
{
    using System;
    using System.ComponentModel.DataAnnotations;
    using System.Data.Entity;
    using System.Data.Entity.ModelConfiguration.Conventions;
    using System.Linq;

    public class FIASContext : DbContext
    {
        // Your context has been configured to use a 'Model' connection string from your application's 
        // configuration file (App.config or Web.config). By default, this connection string targets the 
        // 'FIASLoad.Model' database on your LocalDb instance. 
        // 
        // If you wish to target a different database and/or database provider, modify the 'Model' 
        // connection string in the application configuration file.
        public FIASContext()
            : base("name=FIASContext")
        {
        }

        protected override void OnModelCreating(DbModelBuilder modelBuilder)
        {
            base.OnModelCreating(modelBuilder);
            modelBuilder.Conventions.Remove<PluralizingTableNameConvention>();
        }

        // Add a DbSet for each entity type that you want to include in your model. For more information 
        // on configuring and using a Code First model, see http://go.microsoft.com/fwlink/?LinkId=390109.

        public virtual DbSet<ADDROBJ> ADDROBJS { get; set; }
        public virtual DbSet<HOUSE> HOUSES { get; set; }
        public virtual DbSet<ROOM> ROOM { get; set; }
    }

    public class ADDROBJ
    {
        [Key]
        public Guid AOGUID { get; set; }
        public Guid? PARENTGUID { get; set; }
        public string FORMALNAME { get; set; }
        public string OFFNAME { get; set; }
        public string SHORTNAME { get; set; }
        public int? POSTALCODE { get; set; }
        public long? OKATO { get; set; }
        public long? OKTMO { get; set; }
        public int AOLEVEL { get; set; }
        public string KLADRCODE { get; set; }
        public DateTime UPDATEDATE { get; set; }
    }

    public class HOUSE
    {
        [Key]
        public Guid HOUSEGUID { get; set; }
        public Guid AOGUID { get; set; }
        public int? POSTALCODE { get; set; }
        public long? OKATO { get; set; }
        public long? OKTMO { get; set; }
        public DateTime UPDATEDATE { get; set; }
        public string HOUSENUM { get; set; }
        public int ESTSTATUS { get; set; }
        public string BUILDNUM { get; set; }
        public string STRUCNUM { get; set; }
        public int STRSTATUS { get; set; }
    }

    public class ROOM
    {
        [Key]
        public Guid ROOMGUID { get; set; }
        public Guid HOUSEGUID { get; set; }
        public int? POSTALCODE { get; set; }
        public DateTime UPDATEDATE { get; set; }
        public string FLATNUMBER { get; set; }
        public int FLATTYPE { get; set; }
        public string ROOMNUMBER { get; set; }
        public int ROOMTYPE { get; set; }
    }
}