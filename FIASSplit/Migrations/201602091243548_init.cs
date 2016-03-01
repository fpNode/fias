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
                        FIASCODE = c.String(),
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
                    })
                .PrimaryKey(t => t.HOUSEGUID);
            
        }
        
        public override void Down()
        {
            DropTable("dbo.HOUSE");
            DropTable("dbo.ADDROBJ");
        }
    }
}
