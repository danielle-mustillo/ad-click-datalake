package com.daniellemustillo.datalake_demo;

/**
 * AdClick
 */

public class AdClick {

    private String eventId;
    private String userId;
	private String adId;
	private String campaignId;
    private String country;
	private String device;
	private String eventTime;

    public String getEventId() {
		return eventId;
	}
	public void setEventId(String eventId) {
		this.eventId = eventId;
	}
	public String getUserId() {
		return userId;
	}
    public void setUserId(String userId) {
		this.userId = userId;
	}
	public String getAdId() {
		return adId;
	}
	public void setAdId(String adId) {
		this.adId = adId;
	}
    public String getCampaign_id() {
		return campaign_id;
	}
	public void setCampaign_id(String campaign_id) {
		this.campaign_id = campaign_id;
	}
	public String getCountry() {
		return country;
	}
    public void setCountry(String country) {
		this.country = country;
	}
	public String getDevice() {
		return device;
	}
	public void setDevice(String device) {
		this.device = device;
	}
	public String getEventTime() {
		return eventTime;
	}
	public void setEventTime(String eventTime) {
		this.eventTime = eventTime;
	}
}
