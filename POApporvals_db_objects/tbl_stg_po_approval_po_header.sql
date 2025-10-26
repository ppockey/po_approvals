USE [WebappsDev]
GO

/****** Object:  Table [dbo].[PO_Stg_Header]    Script Date: 10/26/2025 9:20:24 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [dbo].[PO_Stg_Header](
	[PoNumber] [nvarchar](20) NULL,
	[PoDate] [date] NULL,
	[VendorNumber] [nvarchar](20) NULL,
	[VendorName] [nvarchar](80) NULL,
	[VendorAddr1] [nvarchar](80) NULL,
	[VendorAddr2] [nvarchar](80) NULL,
	[VendorAddr3] [nvarchar](80) NULL,
	[VendorState] [nvarchar](20) NULL,
	[VendorPostalCode] [nvarchar](20) NULL,
	[BuyerCode] [nvarchar](10) NULL,
	[BuyerName] [nvarchar](60) NULL,
	[HouseCode] [nvarchar](10) NULL,
	[DirectAmount] [decimal](18, 2) NULL,
	[IndirectAmount] [decimal](18, 2) NULL,
	[CreatedAtUtc] [datetime2](0) NULL
) ON [PRIMARY]
GO


