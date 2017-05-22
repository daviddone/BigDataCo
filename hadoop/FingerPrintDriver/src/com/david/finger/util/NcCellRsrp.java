package com.david.finger.util;
public class NcCellRsrp{
		String enb_id = "";
		String cellid = "";
		int rsrp = 0;
		public NcCellRsrp(String enb_id, String cellid, int rsrp) {
			super();
			this.enb_id = enb_id;
			this.cellid = cellid;
			this.rsrp = rsrp;
		}
		public String getEnb_id() {
			return enb_id;
		}
		public void setEnb_id(String enb_id) {
			this.enb_id = enb_id;
		}
		public String getCellid() {
			return cellid;
		}
		public void setCellid(String cellid) {
			this.cellid = cellid;
		}
		public int getRsrp() {
			return rsrp;
		}
		public void setRsrp(int rsrp) {
			this.rsrp = rsrp;
		}
		
	}