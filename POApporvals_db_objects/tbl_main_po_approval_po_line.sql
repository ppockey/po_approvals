USE [WebappsDev]
GO

/****** Object:  Table [dbo].[PO_Line]    Script Date: 10/26/2025 9:19:41 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [dbo].[PO_Line](
	[PoLineId] [bigint] IDENTITY(1,1) NOT NULL,
	[PoHeaderId] [bigint] NOT NULL,
	[PoNumber] [nvarchar](20) NOT NULL,
	[LineNumber] [int] NOT NULL,
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
	[IsActive] [bit] NOT NULL,
	[DeactivatedAtUtc] [datetime2](0) NULL,
	[DeactivatedBy] [nvarchar](64) NULL,
	[DeactivationReason] [nvarchar](200) NULL,
 CONSTRAINT [PK_PO_Line] PRIMARY KEY CLUSTERED 
(
	[PoLineId] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO

ALTER TABLE [dbo].[PO_Line] ADD  CONSTRAINT [DF_PO_Line_IsActive]  DEFAULT ((1)) FOR [IsActive]
GO

ALTER TABLE [dbo].[PO_Line]  WITH CHECK ADD  CONSTRAINT [FK_PO_Line_Header_PoHeaderId] FOREIGN KEY([PoHeaderId])
REFERENCES [dbo].[PO_Header] ([PoHeaderId])
GO

ALTER TABLE [dbo].[PO_Line] CHECK CONSTRAINT [FK_PO_Line_Header_PoHeaderId]
GO


