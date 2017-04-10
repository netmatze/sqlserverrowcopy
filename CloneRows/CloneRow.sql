CREATE procedure [dbo].[CloneRows@Upd]
	@tableName nvarchar(max),
	@tableOwner nvarchar(max),
	@idColumnName nvarchar(max),
	@fkColumnName nvarchar(max),
	@fkNewValue uniqueidentifier
as
begin
	print 'Cloning individual rows in ' + @tableName
	declare @sql nvarchar(max)
	set @sql = '
	DECLARE cRowsToClone CURSOR LOCAL FOR
		SELECT ' + @idColumnName + '
			FROM #copyCache
	OPEN cRowsToClone
	DECLARE @newPK uniqueidentifier, @oldPK uniqueidentifier
	FETCH NEXT FROM cRowsToClone INTO @oldPK
	While (@@FETCH_STATUS <> -1)
	BEGIN
		select *
			into #oneRow
			from [' + @tableOwner + '].[' + @tableName  + ']
			where ' + @idColumnName + ' = @oldPK						
		set @newPK = newid()
		update #oneRow
			set ' + @fkColumnName + ' = @fkNewValue, ' + @idColumnName + ' = @newPK		
		insert into [' + @tableOwner + '].[' + @tableName  + ']
			select *				
				from #oneRow
		drop table #oneRow

		exec CloneChildrenOfRow@Upd ''' + @tableName + ''', ''' + @tableOwner + ''', @oldPK, @newPK, 1

		FETCH NEXT FROM cRowsToClone INTO @oldPK
	END
	CLOSE cRowsToClone;
	DEALLOCATE cRowsToClone
	'	
	exec sp_executesql @sql, N'@fkNewValue uniqueidentifier', @fkNewValue = @fkNewValue;
end