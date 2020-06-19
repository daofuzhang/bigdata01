package com.want.bigdata.service;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Map;

import org.apache.ibatis.session.SqlSession;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Configurable;
import org.springframework.stereotype.Service;

@Service
@Configurable
public class DbService {

	private SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
	@Autowired
	private SqlSession sqlSession;
	
	private Connection getConn() throws SQLException {
		return sqlSession.getConfiguration().getEnvironment().getDataSource().getConnection();
	}
	private void closeConn(Connection conn, PreparedStatement ps, ResultSet rs) {
		try {
			if (rs != null)
				rs.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		try {
			if (ps != null)
				ps.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		try {
			if (conn != null)
				conn.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	public void test() {
		Connection conn = null;
		PreparedStatement ps = null;
		ResultSet rs = null;
		Map<String, String> map = null;
		try {
			conn = getConn();
			conn.setAutoCommit(false);
			ps = conn.prepareStatement("select count(1) as names from dual");
			rs= ps.executeQuery();
			while(rs.next()) {
				System.out.println(rs.getString("names"));
			}
			
		}catch (Exception e) {
			try {
				conn.rollback();
			} catch (SQLException ex) {
				ex.printStackTrace();
			}
		} finally {
			closeConn(conn, ps, rs);
		}
		
	}
	
}
