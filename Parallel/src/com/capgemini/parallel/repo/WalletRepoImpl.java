package com.capgemini.parallel.repo;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import com.capgemini.parallel.beans.Customer;
import com.capgemini.parallel.beans.Wallet;

public class WalletRepoImpl implements WalletRepo {
	Connection con;

	public WalletRepoImpl() {
		try {
			Class.forName("com.mysql.jdbc.Driver");
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
		try {
			con = DriverManager.getConnection("jdbc:mysql://localhost:3306/test", "root", "corp123");
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	public boolean save(Customer customer) {
		if (customer != null) {
			if (findOne(customer.getMobileNo()) == null) {
				Statement statement = null;
				try {
					statement = con.createStatement();
					String sql = "insert into cust1 values('" + customer.getName() + "', "
							+ customer.getMobileNo() + ", " + customer.getWallet().getBalance() + ")";
					statement.executeUpdate(sql);
					return true;
				} catch (SQLException e) {
					e.printStackTrace();
				}
			} else {
				Statement statement = null;
				try {
					statement = con.createStatement();
					String sql = "update cust1 set balance=" + customer.getWallet().getBalance()
							+ " where mobile=" + customer.getMobileNo();
					statement.executeUpdate(sql);
					return true;
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}
		return false;
	}

	public Customer findOne(String mobileNo) {
		try {
			Statement statement = con.createStatement();
			ResultSet rs = statement.executeQuery("select name,balance from cust1 where mobile=" + mobileNo);
			if (rs.next() != false) {
				Customer c = new Customer();
				c.setName(rs.getString("name"));
				c.setMobileNo(mobileNo);
				c.setWallet(new Wallet(new BigDecimal(rs.getInt("balance"))));
				return c;
			} else {
				return null;
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return null;
	}
}
