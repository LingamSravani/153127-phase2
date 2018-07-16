package com.capgemini.parallel.service;

import java.math.BigDecimal;
import java.sql.Connection;

import com.capgemini.parallel.beans.Customer;
import com.capgemini.parallel.beans.Wallet;
import com.capgemini.parallel.exception.InsufficientBalanceException;
import com.capgemini.parallel.exception.InvalidInputException;
import com.capgemini.parallel.repo.WalletRepo;
import com.capgemini.parallel.repo.WalletRepoImpl;

public class WalletServiceImpl implements WalletService {

	private WalletRepo repo;
	BigDecimal minBalance = new BigDecimal("500");
	BigDecimal tempBalance;
	Connection con;

	public WalletServiceImpl() {
		repo = new WalletRepoImpl();
	}

	public Customer createAccount(String name, String mobileNo,
			BigDecimal amount) {
		while (true) {
			if (validateName(name)) {
				break;
			} else {
				throw new InvalidInputException("Invalid Name");
			}
		}
		while (true) {
			if (validateMobileNumber(mobileNo)) {
				break;
			} else {
				throw new InvalidInputException("Invalid Mobile Number");
			}
		}
		while (true) {
			if (validateAmount(amount.toString())) {
				break;
			} else {
				throw new InvalidInputException("Invalid Amount");
			}
		}
		Customer cust = repo.findOne(mobileNo);
		if (cust != null) {
			throw new InvalidInputException("Duplicate mobile number.");
		}
		try {
			Wallet wallet = new Wallet(amount);
			Customer customer = new Customer(name, mobileNo, wallet);
			boolean temp = repo.save(customer);
			if (temp) {
				return customer;
			}
			return null;
		} catch (InvalidInputException e) {
			throw new InvalidInputException("Invalid Input" + e);
		}
	}

	public Customer showBalance(String mobileNo) {
		Customer customer = repo.findOne(mobileNo);
		if (customer != null)
			return customer;
		else
			throw new InvalidInputException("Invalid mobile no ");
	}

	public Customer fundTransfer(String sourceMobileNo, String targetMobileNo,
			BigDecimal amount) {
		while (true) {
			if (validateAmount(amount.toString())) {
				break;
			} else {
				throw new InvalidInputException("Invalid Amount");
			}
		}
		if (repo.findOne(sourceMobileNo) == null) {
			throw new InvalidInputException("Invalid source mobile number ");
		} else if (repo.findOne(targetMobileNo) == null) {
			throw new InvalidInputException("Invalid target mobile number ");
		} else {
			Customer sourceCustomer = repo.findOne(sourceMobileNo);
			Customer targetCustomer = repo.findOne(targetMobileNo);
			tempBalance = sourceCustomer.getWallet().getBalance();
			BigDecimal tempBalance = sourceCustomer.getWallet().getBalance();
			if (tempBalance.max(minBalance).equals(new BigDecimal("500"))) {
				throw new InsufficientBalanceException("Insufficient Balance.");
			} else if (tempBalance.subtract(amount).max(minBalance)
					.equals(minBalance)) {
				throw new InsufficientBalanceException("Overdraft limit error.");
			} else {
				Wallet sourceWallet = new Wallet(sourceCustomer.getWallet()
						.getBalance().subtract(amount));
				Wallet targetWallet = new Wallet(targetCustomer.getWallet()
						.getBalance().add(amount));
				sourceCustomer.setWallet(sourceWallet);
				targetCustomer.setWallet(targetWallet);
				if (repo.save(sourceCustomer) && repo.save(targetCustomer)) {
					return sourceCustomer;
				} else {
					return null;
				}
			}
		}
	}

	public Customer depositAmount(String mobileNo, BigDecimal amount) {
		Customer customer = repo.findOne(mobileNo);
		if (customer != null) {

			Wallet wallet = new Wallet(customer.getWallet().getBalance()
					.add(amount));
			customer.setWallet(wallet);
			if (repo.save(customer)) {
				return customer;
			} else {
				return null;
			}
		} else
			throw new InvalidInputException("Invalid mobile no ");
	}

	public Customer withdrawAmount(String mobileNo, BigDecimal amount) {
		Customer customer = repo.findOne(mobileNo);
		if (customer != null) {
			BigDecimal tempBalance = customer.getWallet().getBalance();
			if (tempBalance.max(minBalance).equals(new BigDecimal("500"))) {
				throw new InsufficientBalanceException("Insufficient Balance.");
			} else if (tempBalance.subtract(amount).max(minBalance)
					.equals(minBalance)) {
				throw new InsufficientBalanceException("Overdraft limit error.");
			} else {
				Wallet wallet = new Wallet(customer.getWallet().getBalance()
						.subtract(amount));
				customer.setWallet(wallet);
				if (repo.save(customer)) {
					return customer;
				} else {
					return null;
				}
			}
		} else
			throw new InvalidInputException("Invalid mobile no ");
	}

	private boolean validateName(String str) {
		String pattern = "[A-Z][a-z]{3,50}";

		if (str.matches(pattern)) {
			return true;
		} else {
			return false;
		}
	}

	private boolean validateMobileNumber(String str) {
		String pattern = "[1-9][0-9]{9,9}";

		if (str.matches(pattern)) {
			return true;
		} else {
			return false;
		}
	}

	private boolean validateAmount(String str) {
		String pattern = "[1-9][0-9]{2,9}";

		if (str.matches(pattern)) {
			return true;
		} else {
			return false;
		}
	}
}