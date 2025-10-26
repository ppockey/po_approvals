USE [WebappsDev]
GO

/****** Object:  Table [dbo].[PO_ApprovalChain]    Script Date: 10/26/2025 9:14:32 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [dbo].[PO_ApprovalChain](
	[PoNumber] [nvarchar](20) NOT NULL,
	[CreatedAtUtc] [datetime2](0) NOT NULL,
	[Status] [char](1) NOT NULL,
	[FinalizedAtUtc] [datetime2](0) NULL,
 CONSTRAINT [PK_PO_ApprovalChain] PRIMARY KEY CLUSTERED 
(
	[PoNumber] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO

ALTER TABLE [dbo].[PO_ApprovalChain] ADD  CONSTRAINT [DF_PO_ApprovalChain_CreatedAtUtc]  DEFAULT (sysutcdatetime()) FOR [CreatedAtUtc]
GO

ALTER TABLE [dbo].[PO_ApprovalChain] ADD  CONSTRAINT [DF_PO_ApprovalChain_Status]  DEFAULT ('P') FOR [Status]
GO


