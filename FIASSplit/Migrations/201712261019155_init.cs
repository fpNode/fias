namespace FIASSplit.Migrations
{
    using System;
    using System.Data.Entity.Migrations;
    
    public partial class init : DbMigration
    {
        public override void Up()
        {
            CreateTable(
                "dbo.ADDROBJ",
                c => new
                    {
                        AOGUID = c.Guid(nullable: false),
                        PARENTGUID = c.Guid(),
                        FORMALNAME = c.String(),
                        OFFNAME = c.String(),
                        SHORTNAME = c.String(),
                        POSTALCODE = c.Int(),
                        OKATO = c.Long(),
                        OKTMO = c.Long(),
                        AOLEVEL = c.Int(nullable: false),
                        KLADRCODE = c.String(),
                        UPDATEDATE = c.DateTime(nullable: false),
                    })
                .PrimaryKey(t => t.AOGUID);
            
            CreateTable(
                "dbo.HOUSE",
                c => new
                    {
                        HOUSEGUID = c.Guid(nullable: false),
                        AOGUID = c.Guid(nullable: false),
                        POSTALCODE = c.Int(),
                        OKATO = c.Long(),
                        OKTMO = c.Long(),
                        UPDATEDATE = c.DateTime(nullable: false),
                        HOUSENUM = c.String(),
                        ESTSTATUS = c.Int(nullable: false),
                        BUILDNUM = c.String(),
                        STRUCNUM = c.String(),
                        STRSTATUS = c.Int(nullable: false),
                    })
                .PrimaryKey(t => t.HOUSEGUID);
            
            CreateTable(
                "dbo.ROOM",
                c => new
                    {
                        ROOMGUID = c.Guid(nullable: false),
                        HOUSEGUID = c.Guid(nullable: false),
                        POSTALCODE = c.Int(),
                        UPDATEDATE = c.DateTime(nullable: false),
                        FLATNUMBER = c.String(),
                        FLATTYPE = c.Int(nullable: false),
                        ROOMNUMBER = c.String(),
                        ROOMTYPE = c.Int(nullable: false),
                    })
                .PrimaryKey(t => t.ROOMGUID);
            
        }
        
        public override void Down()
        {
            DropTable("dbo.ROOM");
            DropTable("dbo.HOUSE");
            DropTable("dbo.ADDROBJ");
        }
    }
}
