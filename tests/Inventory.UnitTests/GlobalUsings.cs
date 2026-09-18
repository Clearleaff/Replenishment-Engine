global using System;
global using eShop.Inventory.API.Model;
global using eShop.Inventory.API.Application;
global using eShop.Inventory.API.Infrastructure;
global using Microsoft.VisualStudio.TestTools.UnitTesting;

[assembly: Parallelize(Workers = 0, Scope = ExecutionScope.MethodLevel)]
