USE [WebappsDev]
GO

/****** Object:  Table [dbo].[PO_ApproverDirectory]    Script Date: 10/26/2025 9:16:44 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [dbo].[PO_ApproverDirectory](
	[RoleCode] [nvarchar](40) NOT NULL,
	[HouseCode] [nvarchar](10) NOT NULL,
	[BuyerCode] [nvarchar](10) NOT NULL,
	[UserId] [nvarchar](100) NOT NULL,
	[DisplayName] [nvarchar](120) NULL,
	[Email] [nvarchar](256) NULL,
	[IsActive] [bit] NOT NULL,
	[UpdatedAtUtc] [datetime2](0) NOT NULL,
 CONSTRAINT [PK_PO_ApproverDirectory] PRIMARY KEY CLUSTERED 
(
	[RoleCode] ASC,
	[HouseCode] ASC,
	[BuyerCode] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO

ALTER TABLE [dbo].[PO_ApproverDirectory] ADD  DEFAULT ('GLOBAL') FOR [HouseCode]
GO

ALTER TABLE [dbo].[PO_ApproverDirectory] ADD  DEFAULT ('') FOR [BuyerCode]
GO

ALTER TABLE [dbo].[PO_ApproverDirectory] ADD  CONSTRAINT [DF_PO_ApproverDirectory_IsActive]  DEFAULT ((1)) FOR [IsActive]
GO

ALTER TABLE [dbo].[PO_ApproverDirectory] ADD  CONSTRAINT [DF_PO_ApproverDirectory_UpdatedAtUtc]  DEFAULT (sysutcdatetime()) FOR [UpdatedAtUtc]
GO


