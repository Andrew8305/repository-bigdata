package org.platform.titan.modules.qq;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import org.platform.utils.jdbc.JDBCUtils;
import org.platform.utils.jdbc.ResultSetHandler;

public class SQLServerReader {

	public void read() {
		String sql = "select top 100 * from [GroupData10].[dbo].[Group901]";
		JDBCUtils.execute(sql, new ResultSetHandler() {

			@Override
			public Object handle(ResultSet resultSet) {
				try {
					ResultSetMetaData rsmd = resultSet.getMetaData();
					int columnCount = rsmd.getColumnCount();
					for (int i = 1; i <= columnCount; i++) {
						System.out.println(rsmd.getColumnName(i));
						System.out.println(rsmd.getColumnType(i));
						//4 Integer 12 Varchar
					}
					StringBuilder sb = null;
					while (resultSet.next()) {
						sb = new StringBuilder();
						sb.append(resultSet.getInt("ID")).append("-");
						sb.append(resultSet.getInt("QQNum")).append("-");
						sb.append(resultSet.getString("Nick")).append("-");
						sb.append(resultSet.getInt("Age")).append("-");
						sb.append(resultSet.getInt("Gender")).append("-");
						sb.append(resultSet.getInt("Auth")).append("-");
						sb.append(resultSet.getInt("QunNum"));
						System.out.println(sb.toString());
					}
				} catch (SQLException e) {
					e.printStackTrace();
				}
				return null;
			}
		});
	}
	
}
