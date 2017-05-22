package com.david.finger.util;
public class NcCellRsrpSortBean{
	private int ncRsrpSum;
	private int ncRsrpCnt;
	private double ncRsrpAvg;
	
	public NcCellRsrpSortBean(int ncRsrpSum, int ncRsrpCnt, double ncRsrpAvg) {
		super();
		this.ncRsrpSum = ncRsrpSum;
		this.ncRsrpCnt = ncRsrpCnt;
		this.ncRsrpAvg = ncRsrpAvg;
	}
	public int getNcRsrpSum() {
		return ncRsrpSum;
	}
	public void setNcRsrpSum(int ncRsrpSum) {
		this.ncRsrpSum = ncRsrpSum;
	}
	public int getNcRsrpCnt() {
		return ncRsrpCnt;
	}
	public void setNcRsrpCnt(int ncRsrpCnt) {
		this.ncRsrpCnt = ncRsrpCnt;
	}
	public double getNcRsrpAvg() {
		return ncRsrpAvg;
	}
	public void setNcRsrpAvg(int ncRsrpAvg) {
		this.ncRsrpAvg = ncRsrpAvg;
	}
	
	
}