package com.capgemini.parallel.pl;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.Scanner;

import com.capgemini.parallel.beans.Customer;
import com.capgemini.parallel.exception.InvalidInputException;
import com.capgemini.parallel.service.WalletService;
import com.capgemini.parallel.service.WalletServiceImpl;

public class Client {

	public static void main(String[] args) throws InvalidInputException {
		WalletService service=new WalletServiceImpl();
		
		// List myList;
		Scanner sc = new Scanner(System.in);
		String choice;
		int num;
		do {

			System.out.println("1.create account:   ");
			System.out.println("2.show balance:  ");
			System.out.println("3.withdraw: ");
			System.out.println("4.Deposit:  ");
			System.out.println("5.Fund Transfer:   ");
			System.out.println("6.Transactions:    ");
			System.out.println("enter num:  ");
			num = sc.nextInt();
			switch (num) {
			case 1:
				System.out.println("Transaction date:    " + LocalDateTime.now());
				System.out.println("enter account details");
				System.out.println("enter Name:   ");
				String name = sc.next();
				System.out.println("enter mobile no.:  ");
				String mobileno = sc.next();
				System.out.println("enter Bal:   ");
				BigDecimal balance = sc.nextBigDecimal();
				Customer customer = new Customer();
				customer = service.createAccount(name, mobileno, balance);
				System.out.println(customer);

				break;
			case 2:
				System.out.println("enter mobileno:   ");
				String mobile = sc.next();
				Customer cust = service.showBalance(mobile);
				System.out.println(cust);
				break;
			case 3:
				System.out.println("enter mobileno:   ");
				String mobilenum = sc.next();
				System.out.println("enter amount to be withdrawn:   ");
				BigDecimal amount = sc.nextBigDecimal();
				Customer c = service.withdrawAmount(mobilenum, amount);
				System.out.println(c);
				break;
			case 4:
				System.out.println("enter mobileno:   ");
				String mobilenum1 = sc.next();
				System.out.println("enter amount to be deposited:   ");
				BigDecimal amount1 = sc.nextBigDecimal();
				Customer c1 = service.depositAmount(mobilenum1, amount1);
				System.out.println(c1);
				break;
			case 5:
				System.out.println("enter source moblileno:   ");
				String mob1 = sc.next();
				System.out.println("enter traget mobileno:   ");
				String mob2 = sc.next();
				System.out.println("enter amount :   ");
				BigDecimal amt = sc.nextBigDecimal();
				Customer c2 = service.fundTransfer(mob1, mob2, amt);
				System.out.println(c2);
			}
			System.out.println("for continue-yes/y");
			choice = sc.next();
		} while (choice.equalsIgnoreCase("yes") ||choice.equalsIgnoreCase("y"));
	}
}
