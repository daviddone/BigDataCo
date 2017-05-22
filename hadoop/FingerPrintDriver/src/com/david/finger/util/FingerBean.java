package com.david.finger.util;

import java.util.ArrayList;
import java.util.List;

public class FingerBean {
	private String gridInfo;
	private String scEnbId;
	private String scCellId;
	private int scRsrp;
	private List<NcCellRsrp> ncCellRsrpList = new ArrayList<NcCellRsrp>();

	public String getGridInfo() {
		return gridInfo;
	}

	public void setGridInfo(String gridInfo) {
		this.gridInfo = gridInfo;
	}

	public String getScEnbId() {
		return scEnbId;
	}

	public void setScEnbId(String scEnbId) {
		this.scEnbId = scEnbId;
	}

	public String getScCellId() {
		return scCellId;
	}

	public void setScCellId(String scCellId) {
		this.scCellId = scCellId;
	}

	public int getScRsrp() {
		return scRsrp;
	}

	public void setScRsrp(int scRsrp) {
		this.scRsrp = scRsrp;
	}

	public List<NcCellRsrp> getNcCellRsrpList() {
		return ncCellRsrpList;
	}

	public void setNcCellRsrpList(List<NcCellRsrp> ncCellRsrp) {
		this.ncCellRsrpList = ncCellRsrp;
	}

}
