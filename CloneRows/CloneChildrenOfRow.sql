CREATE procedure [dbo].[CloneChildrenOfRow@Upd]
	@tableName sysname,
	@tableOwner sysname,
	@oldId uniqueidentifier,
	@newId uniqueidentifier = null,
	@Level int = 0
as
begin
	set nocount on;
	
	if(@newId is null)
	begin
		set @newId = newid();
	end

	print '** Cloning table ' + @tableName
		+ ', from id ' + cast(@oldId as nvarchar(36))
		+ ' to ' + cast(@newId as nvarchar(36));
	print '----'
	
	declare @sql nvarchar(max), 
			@newIdNVarchar nvarchar(36)
	
	set @newIdNVarchar = cast(@newid as nvarchar(36));

	declare @tempPrimaryKey table (
		table_qualifier nvarchar(max),
		table_owner nvarchar(max),
		table_name nvarchar(max),
		column_name nvarchar(max),
		key_seq int,
		pk_name nvarchar(max)
	)
	
	declare @foreignKeyTables table (
		foreignKeyTableOwner nvarchar(max),
		foreignKeyTableName nvarchar(max),
		foreignKeyColumnName nvarchar(max)
	)
	IF object_id('tempdb..#foreignKeyTables') IS NOT NULL
	BEGIN
	   DROP TABLE #foreignKeyTables
	END
	create table #foreignKeyTables (
		foreignKeyTableOwner nvarchar(max),
		foreignKeyTableName nvarchar(max),
		foreignKeyColumnName nvarchar(max)
	);

	execute GetForeignKeysReferencing@Sel @tableName, '#foreignKeyTables'
	insert into @foreignKeyTables(foreignKeyTableOwner, foreignKeyTableName, foreignKeyColumnName)
		select distinct @tableOwner, foreignKeyTableName, foreignKeyColumnName from #foreignKeyTables;

	--select * from @foreignKeyTables
--end

	if(@Level = 0)
	begin
		IF OBJECT_ID('tempdb..#AdditionalCloning') IS NOT NULL
		begin
			insert @foreignKeyTables (foreignKeyTableOwner, foreignKeyTableName, foreignKeyColumnName)
			select 
				TableOwner,
				TableName, 
				KeyColumnName
			from 
				#AdditionalCloning
			where 
				TableName not in (select foreignKeyTableName from @foreignKeyTables)		
		end
	end

	--select * from @foreignKeyTables

	declare cForeignKeyTables cursor local for
		select foreignKeyTableOwner, foreignKeyTableName, foreignKeyColumnName
			from @foreignKeyTables
	open cForeignKeyTables
	declare @foreignKeyTableOwner nvarchar(max), @foreignKeyTableName nvarchar(max), @foreignKeyColumnName nvarchar(max)
	fetch next from cForeignKeyTables into @foreignKeyTableOwner, @foreignKeyTableName, @foreignKeyColumnName
	 
	while (@@fetch_status <> -1)
	begin
		IF OBJECT_ID('tempdb..#AdditionalCloning') IS NOT NULL
		begin
			if exists (select null from #ExcludeFromCloning where tableName like @foreignKeyTableName)
			begin
				print 'Skipping subtable: ' + @foreignKeyTableOwner + '.' + @foreignKeyTableName + '; in exclude list.'
			
				print '----'
				fetch next from cForeignKeyTables into @foreignKeyTableOwner, @foreignKeyTableName,@foreignKeyColumnName
				continue
			end
		end
		print 'Cloning subtable: ' + @foreignKeyTableOwner + '.' + @foreignKeyTableName  + ', with foreign key ' + @foreignKeyColumnName
		insert into @tempPrimaryKey exec sp_Pkeys @foreignKeyTableName, @foreignKeyTableOwner

		if exists (select null from @tempPrimaryKey)
		begin
			print @foreignKeyTableOwner + '.' + @foreignKeyTableName  + ' has primary keys, deep cloning all rows from ' + @foreignKeyColumnName + ' '
				+ cast(@oldId as nvarchar(36)) + ' to ' + cast(@newId as nvarchar(36))
			declare @pKeyName nvarchar(max)
			select @pKeyName = column_name from @tempPrimaryKey
			set @sql = '
				select ' + @pKeyName + '
					into #copyCache 
					from [' + @foreignKeyTableOwner + '].[' + @foreignKeyTableName  + ']
					where ' + @foreignKeyColumnName + ' = ''' + cast(@oldId as nvarchar(36)) + '''' +
			'				
				exec CloneRows@Upd @foreignKeyTableName, @foreignKeyTableOwner, @pKeyName, @foreignKeyColumnName, @newId;
			'
			--print @sql;			
			exec sp_executesql @sql
				,N'@foreignKeyTableName nvarchar(max), @foreignKeyTableOwner nvarchar(max), @pKeyName nvarchar(max), @foreignKeyColumnName nvarchar(max), @newId uniqueidentifier'
				,@foreignKeyTableName, @foreignKeyTableOwner, @pKeyName, @foreignKeyColumnName, @oldId
		end
		else
		begin			
			declare @referencedTables table (
				primaryKeyTableOwner nvarchar(max)
				,primaryKeyTableName nvarchar(max)
				,primaryKeyColumnName nvarchar(max)
			)
			IF object_id('tempdb..#referencedTables') IS NOT NULL
			BEGIN
			   DROP TABLE #referencedTables
			END
			create table #referencedTables (
				primaryKeyTableOwner nvarchar(max)
				,primaryKeyTableName nvarchar(max)
				,primaryKeyColumnName nvarchar(max)
			)
			delete @referencedTables
			execute GetForeignKeys@Sel @foreignKeyTableName, @foreignKeyTableOwner, '#referencedTables'
			insert into @referencedTables(primaryKeyTableOwner, primaryKeyTableName, primaryKeyColumnName)
				select distinct primaryKeyTableOwner, primaryKeyTableName, primaryKeyColumnName from #referencedTables
			if exists (select null
				from @foreignKeyTables
				where foreignKeyTableName in (select primaryKeyTableName from @referencedTables)
			)
			begin
				declare @t nvarchar(max)
				select @t = foreignKeyTableName
					from @foreignKeyTables
					where foreignKeyTableName in (select primaryKeyTableName from @referencedTables)
				
				print 'WARNING: '
				+ @foreignKeyTableName
				+ ' references other subtables(' + @t
				+ '); skipping this table until we get to subtables.'
				
				print '----'
				fetch next from cForeignKeyTables into @foreignKeyTableOwner, @foreignKeyTableName, @foreignKeyColumnName
				continue
			end
			print @foreignKeyTableOwner + '.' + @foreignKeyTableName
				+ ' has no primary keys, cloning all rows from ' + @foreignKeyColumnName + ' '
				+ cast(@oldId as nvarchar(36)) + ' to ' + @newIdNVarchar
			declare @columns nvarchar(max)
			set @columns = ''
			select @columns = @columns + ('[' + name + ']') + ','
				from sys.columns
				where is_computed = 0
				and object_id = object_id(@foreignKeyTableName)
			set @columns = substring(@columns,0,len(@columns))
			set @sql = '
				select ' + @columns + '
					into #copyCache' + @foreignKeyTableName + '
					from [' + @foreignKeyTableOwner + '].[' + @foreignKeyTableName + ']
					where ' + @foreignKeyColumnName + ' = ''' + cast(@oldId as nvarchar(36)) + '''
				update #copyCache' + @foreignKeyTableName + '
					set ' + @foreignKeyColumnName + ''' = ' + @newIdNVarchar + '''
				insert into [' + @foreignKeyTableOwner + '].[' + @foreignKeyTableName + '] (' + @columns + ')
					select ' + @columns + '
						from #copyCache' + @foreignKeyTableName + '
				drop table #copyCache' + @foreignKeyTableName + '
			'
			print @sql;
			exec sp_executesql @sql;
		end
		delete @tempPrimaryKey;
		
		print '----'
		fetch next from cForeignKeyTables into @foreignKeyTableOwner, @foreignKeyTableName,@foreignKeyColumnName
	end
	close cForeignKeyTables;
	deallocate cForeignKeyTables
end

