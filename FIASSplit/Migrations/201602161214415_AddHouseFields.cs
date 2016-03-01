namespace FIASSplit.Migrations
{
    using System;
    using System.Data.Entity.Migrations;
    
    public partial class AddHouseFields : DbMigration
    {
        public override void Up()
        {
            AddColumn("dbo.HOUSE", "ESTSTATUS", c => c.Int(nullable: false));
            AddColumn("dbo.HOUSE", "BUILDNUM", c => c.String());
            AddColumn("dbo.HOUSE", "STRUCNUM", c => c.String());
            AddColumn("dbo.HOUSE", "STRSTATUS", c => c.Int(nullable: false));
        }
        
        public override void Down()
        {
            DropColumn("dbo.HOUSE", "STRSTATUS");
            DropColumn("dbo.HOUSE", "STRUCNUM");
            DropColumn("dbo.HOUSE", "BUILDNUM");
            DropColumn("dbo.HOUSE", "ESTSTATUS");
        }
    }
}
