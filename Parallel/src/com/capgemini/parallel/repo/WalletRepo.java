package com.capgemini.parallel.repo;

import com.capgemini.parallel.beans.Customer;

public interface WalletRepo {
	public boolean save(Customer customer);

	public Customer findOne(String mobileNo);
}
