USE [WebappsDev]
GO

/****** Object:  Table [dbo].[PO_Stg_Line]    Script Date: 10/26/2025 9:20:56 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [dbo].[PO_Stg_Line](
	[PoNumber] [nvarchar](20) NULL,
	[LineNumber] [int] NULL,
	[HouseCode] [nvarchar](10) NULL,
	[ItemNumber] [nvarchar](40) NULL,
	[ItemDescription] [nvarchar](120) NULL,
	[ItemShortDescription] [nvarchar](60) NULL,
	[QuantityOrdered] [decimal](18, 4) NULL,
	[OrderUom] [nvarchar](12) NULL,
	[UnitCost] [decimal](18, 4) NULL,
	[ExtendedCost] [decimal](18, 4) NULL,
	[RequiredDate] [date] NULL,
	[GlAccount] [nvarchar](40) NULL,
	[CreatedAtUtc] [datetime2](0) NULL
) ON [PRIMARY]
GO


