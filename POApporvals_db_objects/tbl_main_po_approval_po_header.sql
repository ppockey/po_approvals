USE [WebappsDev]
GO

/****** Object:  Table [dbo].[PO_Header]    Script Date: 10/26/2025 9:18:38 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [dbo].[PO_Header](
	[PoHeaderId] [bigint] IDENTITY(1,1) NOT NULL,
	[PoNumber] [nvarchar](20) NOT NULL,
	[PoDate] [date] NULL,
	[VendorNumber] [nvarchar](20) NOT NULL,
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
	[CreatedAtUtc] [datetime2](0) NULL,
	[IsActive] [bit] NOT NULL,
	[DeactivatedAtUtc] [datetime2](0) NULL,
	[DeactivatedBy] [nvarchar](64) NULL,
	[DeactivationReason] [nvarchar](200) NULL,
	[Status] [char](1) NOT NULL,
 CONSTRAINT [PK_PO_Header] PRIMARY KEY CLUSTERED 
(
	[PoHeaderId] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO

ALTER TABLE [dbo].[PO_Header] ADD  CONSTRAINT [DF_PO_Header_IsActive]  DEFAULT ((1)) FOR [IsActive]
GO

ALTER TABLE [dbo].[PO_Header] ADD  CONSTRAINT [DF_PO_Header_Status]  DEFAULT ('W') FOR [Status]
GO


