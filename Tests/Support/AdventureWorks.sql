-- Minimal AdventureWorks subset for testing
CREATE TABLE [dbo].[Product] (
    [ProductID] [int] IDENTITY(1,1) NOT NULL,
    [Name] [nvarchar](50) NOT NULL,
    [ProductNumber] [nvarchar](25) NOT NULL,
    [Color] [nvarchar](15) NULL,
    [StandardCost] [money] NOT NULL,
    [ListPrice] [money] NOT NULL,
    [Size] [nvarchar](5) NULL,
    [Weight] [decimal](8, 2) NULL,
 CONSTRAINT [PK_Product_ProductID] PRIMARY KEY CLUSTERED ([ProductID] ASC)
);

CREATE TABLE [dbo].[Customer] (
    [CustomerID] [int] IDENTITY(1,1) NOT NULL,
    [FirstName] [nvarchar](50) NOT NULL,
    [LastName] [nvarchar](50) NOT NULL,
    [CompanyName] [nvarchar](128) NULL,
    [EmailAddress] [nvarchar](50) NULL,
    [Phone] [nvarchar](25) NULL,
 CONSTRAINT [PK_Customer_CustomerID] PRIMARY KEY CLUSTERED ([CustomerID] ASC)
);

INSERT INTO [dbo].[Product] ([Name], [ProductNumber], [Color], [StandardCost], [ListPrice])
VALUES (N'HL Road Frame - Black, 58', N'FR-R92B-58', N'Black', 1059.31, 1431.50);

INSERT INTO [dbo].[Customer] ([FirstName], [LastName], [CompanyName], [EmailAddress])
VALUES (N'Orlando', N'Gee', N'A Bike Store', N'orlando0@adventure-works.com');
