USE [WebappsDev]
GO

/****** Object:  Table [dbo].[PO_ApprovalStage]    Script Date: 10/26/2025 9:15:56 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [dbo].[PO_ApprovalStage](
	[PoNumber] [nvarchar](20) NOT NULL,
	[Sequence] [int] NOT NULL,
	[RoleCode] [nvarchar](40) NOT NULL,
	[ApproverUserId] [nvarchar](100) NULL,
	[Category] [char](1) NULL,
	[ThresholdFrom] [decimal](18, 2) NULL,
	[ThresholdTo] [decimal](18, 2) NULL,
	[Status] [char](1) NOT NULL,
	[DecidedAtUtc] [datetime2](0) NULL,
 CONSTRAINT [PK_PO_ApprovalStage] PRIMARY KEY CLUSTERED 
(
	[PoNumber] ASC,
	[Sequence] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO

ALTER TABLE [dbo].[PO_ApprovalStage] ADD  CONSTRAINT [DF_PO_ApprovalStage_Status]  DEFAULT ('P') FOR [Status]
GO

ALTER TABLE [dbo].[PO_ApprovalStage]  WITH CHECK ADD  CONSTRAINT [FK_PO_ApprovalStage_Chain] FOREIGN KEY([PoNumber])
REFERENCES [dbo].[PO_ApprovalChain] ([PoNumber])
GO

ALTER TABLE [dbo].[PO_ApprovalStage] CHECK CONSTRAINT [FK_PO_ApprovalStage_Chain]
GO


