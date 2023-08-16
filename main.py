# import libraries
from sp_api.base import Marketplaces, ProcessingStatus
from sp_api.api import Reports
import pandas as pd
import numpy as np
import time
import json
import requests
from datetime import datetime
import requests
import gzip
import io
import streamlit as st
from PIL import Image
from io import BytesIO
import zipfile


def generate_ppc_report(
    credentials_df: pd.DataFrame,
    profileId_df: pd.DataFrame,
    productByCampaign: pd.DataFrame,
    profit_df: pd.DataFrame,
    startDate: str,
    endDate: str,
):
    progress_text = (
        "Operation in progress. Please wait."  # define the progress bar text
    )
    progress = 0
    _my_bar = st.progress(
        progress, text=progress_text
    )  # create a progress bar starting from 0 presenting the progress text

    productByCampaign = (
        productByCampaign.drop_duplicates()
    )  # dropping duplicates in the product_campagin df

    profileId_df["profile_id"] = profileId_df["profile_id"].astype(
        str
    )  # define the profile_id as strings

    profileId_df = profileId_df[
        profileId_df["marketplace"].isin(productByCampaign["marketplace"])
    ]  # Filtering the profile ids df to contain only marketplaces with campaigns

    # define the amazon ads credentials
    ADS_REFRESH_TOKEN = credentials_df["ads_api"]["ADS_REFRESH_TOKEN"]
    ADS_CLIENT_ID = credentials_df["ads_api"]["ADS_CLIENT_ID"]
    ADS_CLIENT_SECRET = credentials_df["ads_api"]["ADS_CLIENT_SECRET"]

    ads_headers_refresh = {
        "client_id": ADS_CLIENT_ID,
        "refresh_token": ADS_REFRESH_TOKEN,
        "client_secret": ADS_CLIENT_SECRET,
    }  # define the headers for the access token generation

    access_token = new_access_token(ads_headers_refresh)  # generate new access token
    startDate = startDate  # start date to be filled by GUI
    endDate = endDate  # start date to be filled by GUI

    date_list = create_date_list(
        startDate, endDate
    )  # creating a list of all the dates between the first and last date

    progress_unit = 0.9 / (
        len(date_list) * 4 * len(profileId_df) + 6 * len(profileId_df)
    )  # calculating a progress unit
    progress = 0.05
    _my_bar.progress(
        progress, text=progress_text
    )  # turning the progress bar to 5 precent

    display_reports_id = pd.DataFrame(
        columns=["date", "marketplace", "report_id", "credentials", "url"]
    )
    brand_reports_id = pd.DataFrame(
        columns=["date", "marketplace", "report_id", "credentials", "url"]
    )

    for i in profileId_df.index:
        ads_headers_v2 = {
            "Content-Type": "application/json",
            "Amazon-Advertising-API-ClientId": ADS_CLIENT_ID,
            "Authorization": "Bearer " + access_token,
            "Amazon-Advertising-API-Scope": profileId_df["profile_id"][i],
        }

        for date in date_list:
            display_campaign_report_id = create_display_campaigns_report(
                profileId_df, ads_headers_v2, date, i
            )
            display_reports_id = display_reports_id._append(
                display_campaign_report_id, ignore_index=True
            )
            brand_campaign_report_id = create_brand_campaigns_report(
                profileId_df, ads_headers_v2, date, i
            )
            brand_reports_id = brand_reports_id._append(
                brand_campaign_report_id, ignore_index=True
            )
            progress = progress + progress_unit * 2

        _my_bar.progress(progress, text=progress_text)

    product_campaing_report_ids = pd.DataFrame(
        columns=["report_id", "profile_id", "marketplace", "credentials", "url"]
    )
    for i in profileId_df.index:
        ads_headers = {
            "Content-Type": "application/vnd.createasyncreportrequest.v3+json",
            "Amazon-Advertising-API-ClientId": ADS_CLIENT_ID,
            "Authorization": "Bearer " + access_token,
            "Amazon-Advertising-API-Scope": profileId_df["profile_id"][i],
        }  # header for the report generation

        report_id = create_reportsByCampaign(
            ads_headers,
            profileId_df["url"][i],
            startDate,
            endDate,
        )  # creating a report and getting the report_id

        product_campaing_report_ids = product_campaing_report_ids._append(
            {
                "report_id": report_id,
                "profile_id": profileId_df["profile_id"][i],
                "marketplace": profileId_df["marketplace"][i],
                "credentials": ads_headers,
                "url": profileId_df["url"][i],
            },
            ignore_index=True,
        )
        progress = progress + progress_unit

        _my_bar.progress(progress, text=progress_text)

    report_id_df = pd.DataFrame(columns=["marketplace", "report_id", "credentials"])
    full_business_report = pd.DataFrame()
    for i in profileId_df.index:
        report_id = generate_bussiness_report(
            credentials_df[profileId_df["credentials"][i]].to_dict(),
            startDate,
            endDate,
            profileId_df["marketplace"][i],
        )
        report_id_df = report_id_df._append(
            {
                "marketplace": profileId_df["marketplace"][i],
                "report_id": report_id,
                "credentials": profileId_df["credentials"][i],
            },
            ignore_index=True,
        )
        progress = progress + progress_unit

        _my_bar.progress(progress, text=progress_text)

    full_product_campaign_report = pd.DataFrame()

    st.session_state["sponsered_products_report_ids"] = product_campaing_report_ids
    st.session_state["sponsered_display_report_ids"] = display_reports_id
    st.session_state["sponsered_brand_report_ids"] = brand_reports_id
    st.session_state["bussiness_report_ids"] = report_id_df
    st.write(st.session_state)

    product_campaing_report_ids[
        "got_report"
    ] = ""  # creating an empty column to test the reports gotten

    for i in product_campaing_report_ids.index:
        product_campagin_data_df = get_reportByCampaign(
            product_campaing_report_ids["credentials"][i],
            product_campaing_report_ids["report_id"][i],
            product_campaing_report_ids["url"][i],
        )  # getting the report for the campaign preformence for every marketplace

        product_campagin_data_df["marketplace"] = product_campaing_report_ids[
            "marketplace"
        ][
            i
        ]  # adding a column with the market place to the campaigns

        if (
            full_product_campaign_report is None
        ):  # if there is no df named full_campaign_report
            full_product_campaign_report = product_campagin_data_df  # create one with the first full bussiness report
        else:
            full_product_campaign_report = full_product_campaign_report._append(
                product_campagin_data_df
            )  # append the bussiness report to the full_bussiness_report df
        progress = progress + progress_unit

        _my_bar.progress(progress, text=progress_text)
        product_campaing_report_ids["got_report"][i] = product_campaing_report_ids[
            "marketplace"
        ][i]
    product_campaigns_full_report = full_product_campaign_report

    _my_bar.progress(progress, text=progress_text)
    full_campaign_report = pd.DataFrame()
    full_brand_report = pd.DataFrame()
    brand_reports_id["got_report"] = ""
    display_reports_id[
        "got_report"
    ] = ""  # creating an empty column to test the reports gotten

    for i in display_reports_id.index:
        campagin_data_df = get_campaigns_report_v2(
            display_reports_id, i
        )  # getting the report for the campaign preformence for every marketplace

        if full_campaign_report is None:  # if there is no df named full_campaign_report
            full_campaign_report = (
                campagin_data_df  # create one with the first full bussiness report
            )
        else:
            full_campaign_report = full_campaign_report._append(
                campagin_data_df
            )  # append the bussiness report to the full_bussiness_report df
        display_reports_id["got_report"][i] = display_reports_id["marketplace"][i]
        progress = progress + progress_unit
        _my_bar.progress(progress, text=progress_text)
    display_campaign_full_report = full_campaign_report
    for i in brand_reports_id.index:
        brand_campagin_data_df = get_campaigns_report_v2(
            brand_reports_id, i
        )  # getting the report for the campaign preformence for every marketplace

        if full_brand_report is None:  # if there is no df named full_campaign_report
            full_brand_report = brand_campagin_data_df  # create one with the first full bussiness report
        else:
            full_brand_report = full_brand_report._append(
                brand_campagin_data_df
            )  # append the bussiness report to the full_bussiness_report df
        brand_reports_id["got_report"][i] = brand_reports_id["marketplace"][i]
        progress = progress + progress_unit
        _my_bar.progress(progress, text=progress_text)
    brand_campaign_full_report = full_brand_report

    display_df = display_campaign_full_report.rename(
        columns={
            "cost": "spend",
            "attributedUnitsOrdered14d": "purchases7d",
            "attributedSales14d": "sales7d",
        }
    )  # renaming the columns of the display campaing to match the other reports
    display_df.to_excel("./Display_campaigns.xlsx")
    brand_df = brand_campaign_full_report.rename(
        columns={
            "cost": "spend",
            "attributedConversions14d": "purchases7d",
            "attributedSales14d": "sales7d",
        }
    )  # renaming the columns of the display campaing  to match the other reports
    brand_df.to_excel("./brand_campaigns.xlsx")
    product_campaigns_full_report.to_excel("./product_campaigns.xlsx")
    campaign_df = product_campaigns_full_report._append(display_df)._append(
        brand_df
    )  # appending all the campaign reports together to a single df

    summarizeProductsAndCampaingsGroup = JoinAsinsForCampaigns(
        campaign_df, productByCampaign
    )  # Join the Asins of the products to the campaigns data

    report_id_df["got_report"] = ""
    for i in report_id_df.index:
        business_report = get_bussiness_report(
            credentials_df[report_id_df["credentials"][i]].to_dict(),
            report_id_df["report_id"][i],
            report_id_df["marketplace"][i],
        )
        if full_business_report is None:  # if there is no df named full_campaign_report
            full_business_report = (
                business_report  # create one with the first full bussiness report
            )
        else:
            full_business_report = full_business_report._append(
                business_report
            )  # append the bussiness report to the full_bussiness_report df
        report_id_df["got_report"][i] = report_id_df["marketplace"][i]
        progress = progress + progress_unit

        _my_bar.progress(progress, text=progress_text)

    _my_bar.progress(0.95, progress_text)  # setting the progress bar to 95 precent

    summarizeProductsAndCampaingsGroup["Asin"] = summarizeProductsAndCampaingsGroup[
        "Asin"
    ].str.rstrip()  # striping extra white space on the Asin column

    full_business_report["childAsin"] = full_business_report["childAsin"].str.rstrip()
    full_business_report.to_excel("./Bussiness_report.xlsx")
    summarizeProductsAndCampaingsWithReport = summarizeProductsAndCampaingsGroup.merge(
        full_business_report[
            [
                "unitsOrdered",
                "childAsin",
                "unitSessionPercentage",
                "amount",
                "marketplace",
            ]
        ],
        left_on=["Asin", "marketplace"],
        right_on=["childAsin", "marketplace"],
        how="left",
    )  # joining the ppc report with the  business report on marketplace and Asin

    summarizeProductsAndCampaingsWithReport["UnitPrice"] = (
        summarizeProductsAndCampaingsWithReport["amount"]
        / summarizeProductsAndCampaingsWithReport["unitsOrdered"]
    )  # calculating the price of one unit

    # convert string to date object
    startDatetime = datetime.strptime(startDate, "%Y-%m-%d")
    endDatetime = datetime.strptime(endDate, "%Y-%m-%d")

    # difference between dates in timedelta
    delta = endDatetime - startDatetime

    summarizeProductsAndCampaingsWithReport["period"] = delta / pd.Timedelta(
        days=1
    )  # the period of the report in days

    summarizeProductsAndCampaingsWithReport["period"] = (
        summarizeProductsAndCampaingsWithReport["period"] + 1
    )
    # the period of the report in days

    summarizeProductsAndCampaingsWithReport["DailySales"] = (
        summarizeProductsAndCampaingsWithReport["unitsOrdered"]
        / summarizeProductsAndCampaingsWithReport["period"]
    )  # calculating the units ordered per day

    summarizeProductsAndCampaingsWithReport["ppcCostPerUnit"] = (
        summarizeProductsAndCampaingsWithReport["real_spend"]
        / summarizeProductsAndCampaingsWithReport["real_ordered"]
    )  # calculating the ppc cost per unit

    summarizeProductsAndCampaingsWithReport["Acos"] = (
        summarizeProductsAndCampaingsWithReport["real_spend"]
        / summarizeProductsAndCampaingsWithReport["real_sales"]
    )  # calculating the Acos

    summarizeProductsAndCampaingsWithReport[
        "date"
    ] = datetime.today()  # filling the date column wwith todays date

    summarizeProductsAndCampaingsWithReport = summarizeProductsAndCampaingsWithReport[
        [
            "marketplace",
            "Category",
            "Asin",
            "date",
            "period",
            "real_spend",
            "real_sales",
            "amount",
            "real_ordered",
            "unitsOrdered",
            "DailySales",
            "unitSessionPercentage",
            "ppcCostPerUnit",
            "UnitPrice",
            "Acos",
        ]
    ]  # filtering the df for importanet columns
    summarizeProductsAndCampaingsWithReport = (
        summarizeProductsAndCampaingsWithReport.sort_values(by="marketplace")
    )

    summarizeProductsAndCampaingsWithReport = (
        summarizeProductsAndCampaingsWithReport.rename(
            columns={
                "Acos": "Avg Acos %",
                "date": "Check Date",
                "period": "Period",
                "real_spend": "Campaign Spend",
                "UnitPrice": "Sale Price",
                "real_ordered": "Unit Sales From PPC",
                "DailySales": "Unit Daily Sales",
                "ppcCostPerUnit": "PPC Cost Without Organic Sales",
                "unitsOrdered": "Unit Sales Total",
                "unitSessionPercentage": "Unit Session %",
                "amount": "Total Sales",
            }
        )
    )
    # renaming the columns of the df
    summarizeProductsAndCampaingsWithReport[
        "Check Date"
    ] = summarizeProductsAndCampaingsWithReport["Check Date"].dt.date
    summarizeProductsAndCampaingsWithReport["Start Date"] = startDatetime
    summarizeProductsAndCampaingsWithReport[
        "Start Date"
    ] = summarizeProductsAndCampaingsWithReport["Start Date"].dt.date

    summarizeProductsAndCampaingsWithReport["End Date"] = endDatetime
    summarizeProductsAndCampaingsWithReport[
        "End Date"
    ] = summarizeProductsAndCampaingsWithReport["End Date"].dt.date
    summarizeProductsAndCampaingsWithReport["Total PPC Cost Per Unit"] = (
        summarizeProductsAndCampaingsWithReport["Campaign Spend"]
        / summarizeProductsAndCampaingsWithReport["Unit Sales Total"]
    )
    summarizeProductsAndCampaingsWithReport["Rating"] = ""
    summarizeProductsAndCampaingsWithReport["Notes"] = ""

    profit_df["ASIN"] = profit_df[
        "ASIN"
    ].str.rstrip()  # right striping the series for extra spaces

    summarizeProductsAndCampaingsWithReport = (
        summarizeProductsAndCampaingsWithReport.merge(
            profit_df[["profit without PPC", "ASIN", "Country"]],
            left_on=["marketplace", "Asin"],
            right_on=["Country", "ASIN"],
            how="left",
        )  # merging the profit df with the full report based on asin and marketplace
    )

    summarizeProductsAndCampaingsWithReport[
        "Profit Per Unit After PPC ILS"
    ] = summarizeProductsAndCampaingsWithReport[
        "profit without PPC"
    ] - summarizeProductsAndCampaingsWithReport[
        "Total PPC Cost Per Unit"
    ].fillna(
        0
    )  # calculating the profit per unit

    summarizeProductsAndCampaingsWithReport["Profit 30 Days ILS"] = (
        summarizeProductsAndCampaingsWithReport["Profit Per Unit After PPC ILS"]
        * (
            summarizeProductsAndCampaingsWithReport["Unit Sales From PPC"]
            / summarizeProductsAndCampaingsWithReport["Period"]
        )
    ) * 30  # calculating the profit revenue(monthly revenue)

    summarizeProductsAndCampaingsWithReport = summarizeProductsAndCampaingsWithReport[
        [
            "marketplace",
            "Category",
            "Asin",
            "Check Date",
            "Start Date",
            "End Date",
            "Period",
            "Unit Sales Total",
            "Unit Daily Sales",
            "Unit Session %",
            "Sale Price",
            "Rating",
            "Unit Sales From PPC",
            "Campaign Spend",
            "PPC Cost Without Organic Sales",
            "Total PPC Cost Per Unit",
            "Avg Acos %",
            "Profit Per Unit After PPC ILS",
            "Profit 30 Days ILS",
            "Total Sales",
            "profit without PPC",
            "Notes",
        ]
    ]
    profit_df = profit_df[
        [
            "Country",
            "ASIN",
            "Tyroler Code",
            "Product",
            "Price",
            "Currency conversion ILS",
            "amazon shipment fee",
            "product cost",
            "profit without PPC",
            "Shipment cost to amazon",
        ]
    ].merge(
        summarizeProductsAndCampaingsWithReport[
            ["Asin", "marketplace", "Total PPC Cost Per Unit"]
        ],
        how="left",
        left_on=["Country", "ASIN"],
        right_on=["marketplace", "Asin"],
    )  # merging the profit df and the full report on country and asin

    profit_df["Total PPC Cost Per Unit"] = profit_df["Total PPC Cost Per Unit"].fillna(
        0
    )  # filling ppc cost without organic sales na's with 0

    profit_df["profit after ppc"] = (
        profit_df["profit without PPC"] - profit_df["Total PPC Cost Per Unit"]
    )  # calculating the profit after ppc
    profit_df = profit_df.drop_duplicates()  # dropping duplicates from the profit df

    _my_bar.progress(1)

    return summarizeProductsAndCampaingsWithReport, profit_df


def create_get_display_campaign_reports(
    profileId_df: pd.DataFrame,
    ADS_CLIENT_ID: str,
    access_token: str,
    date_list: list,
    _my_bar,
    progress_text,
    progress,
    progress_unit,
):
    display_reports_id = pd.DataFrame(
        columns=["date", "marketplace", "report_id", "credentials", "url"]
    )

    for i in profileId_df.index:
        ads_headers_v2 = {
            "Content-Type": "application/json",
            "Amazon-Advertising-API-ClientId": ADS_CLIENT_ID,
            "Authorization": "Bearer " + access_token,
            "Amazon-Advertising-API-Scope": profileId_df["profile_id"][i],
        }

        for date in date_list:
            display_campaign_report_id = create_display_campaigns_report(
                profileId_df, ads_headers_v2, date, i
            )
            display_reports_id = display_reports_id._append(
                display_campaign_report_id, ignore_index=True
            )
        progress = progress + progress_unit

        _my_bar.progress(progress, text=progress_text)

    full_campaign_report = pd.DataFrame()
    display_reports_id[
        "got_report"
    ] = ""  # creating an empty column to test the reports gotten

    for i in display_reports_id.index:
        campagin_data_df = get_campaigns_report_v2(
            display_reports_id, i
        )  # getting the report for the campaign preformence for every marketplace

        if full_campaign_report is None:  # if there is no df named full_campaign_report
            full_campaign_report = (
                campagin_data_df  # create one with the first full bussiness report
            )
        else:
            full_campaign_report = full_campaign_report._append(
                campagin_data_df
            )  # append the bussiness report to the full_bussiness_report df
        display_reports_id["got_report"][i] = display_reports_id["marketplace"][i]
        progress = progress + progress_unit

        _my_bar.progress(progress, text=progress_text)
    return full_campaign_report, progress


def create_get_brand_campaign_reports(
    profileId_df: pd.DataFrame,
    ADS_CLIENT_ID: str,
    access_token: str,
    date_list: list,
    _my_bar,
    progress_text,
    progress,
    progress_unit,
):
    display_reports_id = pd.DataFrame(
        columns=["date", "marketplace", "report_id", "credentials", "url"]
    )

    for i in profileId_df.index:
        ads_headers_v2 = {
            "Content-Type": "application/json",
            "Amazon-Advertising-API-ClientId": ADS_CLIENT_ID,
            "Authorization": "Bearer " + access_token,
            "Amazon-Advertising-API-Scope": profileId_df["profile_id"][i],
        }

        for date in date_list:
            display_campaign_report_id = create_brand_campaigns_report(
                profileId_df, ads_headers_v2, date, i
            )
            display_reports_id = display_reports_id._append(
                display_campaign_report_id, ignore_index=True
            )
    progress = progress + progress_unit

    _my_bar.progress(progress, text=progress_text)
    full_campaign_report = pd.DataFrame()
    display_reports_id[
        "got_report"
    ] = ""  # creating an empty column to test the reports gotten

    for i in display_reports_id.index:
        campagin_data_df = get_campaigns_report_v2(
            display_reports_id, i
        )  # getting the report for the campaign preformence for every marketplace

        if full_campaign_report is None:  # if there is no df named full_campaign_report
            full_campaign_report = (
                campagin_data_df  # create one with the first full bussiness report
            )

        else:
            full_campaign_report = full_campaign_report._append(
                campagin_data_df
            )  # append the bussiness report to the full_bussiness_report df
        progress = progress + progress_unit

        _my_bar.progress(progress, text=progress_text)

        display_reports_id["got_report"][i] = display_reports_id["marketplace"][i]
    return full_campaign_report, progress


def create_get_business_report(
    profile_id_df: pd.DataFrame,
    startDate: str,
    endDate: str,
    credentials_df: pd.DataFrame,
    _my_bar,
    progress_text,
    progress,
    progress_unit,
):
    report_id_df = pd.DataFrame(columns=["marketplace", "report_id", "credentials"])
    full_business_report = pd.DataFrame()
    for i in profile_id_df.index:
        report_id = generate_bussiness_report(
            credentials_df[profile_id_df["credentials"][i]].to_dict(),
            startDate,
            endDate,
            profile_id_df["marketplace"][i],
        )

        report_id_df = report_id_df._append(
            {
                "marketplace": profile_id_df["marketplace"][i],
                "report_id": report_id,
                "credentials": profile_id_df["credentials"][i],
            },
            ignore_index=True,
        )
        progress = progress + progress_unit

        _my_bar.progress(progress, text=progress_text)
    report_id_df["got_report"] = ""
    for i in report_id_df.index:
        business_report = get_bussiness_report(
            credentials_df[report_id_df["credentials"][i]].to_dict(),
            report_id_df["report_id"][i],
            report_id_df["marketplace"][i],
        )
        if full_business_report is None:  # if there is no df named full_campaign_report
            full_business_report = (
                business_report  # create one with the first full bussiness report
            )
        else:
            full_business_report = full_business_report._append(
                business_report
            )  # append the bussiness report to the full_bussiness_report df
        report_id_df["got_report"][i] = report_id_df["marketplace"][i]
        progress = progress + progress_unit

        _my_bar.progress(progress, text=progress_text)
    return full_business_report, progress


def create_get_product_campaigns(
    profileId_df: pd.DataFrame,
    ADS_CLIENT_ID: str,
    access_token: str,
    startDate: str,
    endDate: str,
    _my_bar,
    progress_text,
    progress,
    progress_unit,
):
    campaing_report_ids = pd.DataFrame(
        columns=["report_id", "profile_id", "marketplace", "credentials", "url"]
    )
    for i in profileId_df.index:
        ads_headers = {
            "Content-Type": "application/vnd.createasyncreportrequest.v3+json",
            "Amazon-Advertising-API-ClientId": ADS_CLIENT_ID,
            "Authorization": "Bearer " + access_token,
            "Amazon-Advertising-API-Scope": profileId_df["profile_id"][i],
        }  # header for the report generation

        report_id = create_reportsByCampaign(
            ads_headers,
            profileId_df["url"][i],
            startDate,
            endDate,
        )  # creating a report and getting the report_id

        campaing_report_ids = campaing_report_ids._append(
            {
                "report_id": report_id,
                "profile_id": profileId_df["profile_id"][i],
                "marketplace": profileId_df["marketplace"][i],
                "credentials": ads_headers,
                "url": profileId_df["url"][i],
            },
            ignore_index=True,
        )
        progress = progress + progress_unit

        _my_bar.progress(progress, text=progress_text)
    full_campaign_report = pd.DataFrame()

    campaing_report_ids[
        "got_report"
    ] = ""  # creating an empty column to test the reports gotten

    for i in campaing_report_ids.index:
        campagin_data_df = get_reportByCampaign(
            campaing_report_ids["credentials"][i],
            campaing_report_ids["report_id"][i],
            campaing_report_ids["url"][i],
        )  # getting the report for the campaign preformence for every marketplace

        campagin_data_df["marketplace"] = campaing_report_ids["marketplace"][
            i
        ]  # adding a column with the market place to the campaigns

        if full_campaign_report is None:  # if there is no df named full_campaign_report
            full_campaign_report = (
                campagin_data_df  # create one with the first full bussiness report
            )
        else:
            full_campaign_report = full_campaign_report._append(
                campagin_data_df
            )  # append the bussiness report to the full_bussiness_report df
        progress = progress + progress_unit

        _my_bar.progress(progress, text=progress_text)
        campaing_report_ids["got_report"][i] = campaing_report_ids["marketplace"][i]
    return full_campaign_report, progress


def new_access_token(credentials):
    data = credentials
    data.update({"grant_type": "refresh_token"})
    print(data)
    response = requests.post("https://api.amazon.com/auth/o2/token", data=data)
    json = response.json()
    print(json)
    return json["access_token"]


def create_date_list(startDate: str, endDate: str) -> list:
    date_range = pd.date_range(startDate, endDate)
    date_list = [int(date.strftime("%Y%m%d")) for date in date_range]
    return date_list


def generate_bussiness_report(credentials, start_date, end_date, marketplace):
    #  gets a range of date and credentials and output a business report
    report_type = "GET_SALES_AND_TRAFFIC_REPORT"
    # stating the type of report
    createReportResponse = Reports(
        credentials=credentials, marketplace=Marketplaces[marketplace]
    ).create_report(  # creating a report
        reportType=report_type,
        dataStartTime=start_date,
        dataEndTime=end_date,
        reportOptions={"asinGranularity": "CHILD"},
    )

    report = createReportResponse.payload  # taking the report response payload
    print(report)
    report_id = report["reportId"]  # extracting the newly created report id

    return report_id


def get_bussiness_report(credentials, report_id, marketplace):
    report_type = "GET_SALES_AND_TRAFFIC_REPORT"
    processing_status = [
        "IN_QUEUE",
        "IN_PROGRESS",
    ]
    data = Reports(
        credentials=credentials, marketplace=Marketplaces[marketplace]
    ).get_report(  # trying to get the report by id
        reportId=report_id,
        reportTypes=report_type,
        processingStatuses=processing_status,
    )
    while data.payload.get(
        "processingStatus"
    ) not in [  # a while loop to check the proccesing status
        ProcessingStatus.DONE,
        ProcessingStatus.FATAL,
        ProcessingStatus.CANCELLED,
    ]:
        print(data.payload)
        print("Sleeping...")
        time.sleep(2)  # sleeping for 2 seconds and then trying again
        data = Reports(
            credentials=credentials, marketplace=Marketplaces[marketplace]
        ).get_report(report_id)

    if data.payload.get(
        "processingStatus"
    ) in [  # if the process as failed print report failed
        ProcessingStatus.FATAL,
        ProcessingStatus.CANCELLED,
    ]:
        full_df = pd.DataFrame({"sku": data.payload, "marketplace": marketplace})

    else:
        print("Success:")  # if the report is ready, get the report document
        print(data.payload)
        report_data = Reports(
            credentials=credentials, marketplace=Marketplaces[marketplace]
        ).get_report_document(
            reportDocumentId=data.payload["reportDocumentId"],
            decrypt=True,
            download=True,
        )

    document_data = (
        report_data.payload
    )  # getting the data from the document to json and then to dataframe
    # print(document_data)
    document_dict = document_data["document"]
    res = json.loads(document_dict)

    df = pd.DataFrame(res["salesAndTrafficByAsin"])
    df["marketplace"] = marketplace
    if "salesByAsin" in df.columns:
        dict_series = df[
            "salesByAsin"
        ]  # creating a sub dataframe with the details of sales by asin
        dict_list = []
        for i in dict_series:
            dict_list.append(i)

        sales_df = pd.DataFrame.from_dict(dict_list)

        dict_series_nested = sales_df[
            "orderedProductSales"
        ]  # creating a sub sub dataframe with the details of the ordered product sales
        dict_list = []
        for i in dict_series_nested:
            dict_list.append(i)

        nested_df = pd.DataFrame.from_dict(dict_list)

        traffic_dict_series = df[
            "trafficByAsin"
        ]  # creating a sub dataframe with the details of traffic by asin
        traffic_dict_list = []
        for i in traffic_dict_series:
            traffic_dict_list.append(i)

        traffic_df = pd.DataFrame.from_dict(traffic_dict_list)
        full_df = (
            df.join(sales_df).join(traffic_df).join(nested_df)
        )  # joining all the diffrent dataframes by index

        return full_df
    else:
        return df


def create_reportsByCampaign(headers, url, startDate, endDate):
    data = (
        '''{
        "name":"SP campaigns report",
        "startDate":"'''
        + startDate
        + """",
        "endDate":"""
        + '"'
        + endDate
        + '"'
        + """,
        "configuration":{
            "adProduct":"SPONSORED_PRODUCTS",
            "groupBy":["campaign"],
            "columns":["spend","clicks","campaignName","purchases7d","sales7d"],
            "reportTypeId":"spCampaigns",
            "timeUnit":"SUMMARY",
            "format":"GZIP_JSON"
            }
            }"""
    )

    print(data)
    response = requests.post(
        url,
        headers=headers,
        data=data,
    )

    print(response.json())
    report_id = response.json()["reportId"]
    return report_id


def get_reportByCampaign(headers, report_id, url):
    response = requests.get(
        url + "/" + report_id,
        headers=headers,
    )
    print(response.json())
    while (response.json()["status"] == "PENDING") | (
        response.json()["status"] == "PROCESSING"
    ):
        print("sleeping...")
        time.sleep(10)
        response = requests.get(
            url + "/" + report_id,
            headers=headers,
        )

    url = response.json()["url"]
    if url == None:
        print(response.json())
        output = pd.DataFrame(response.json())

    print(url)
    response = requests.get(
        url,
    )

    compressed_file = io.BytesIO(response.content)  # extract .gz file
    decompressed_file = gzip.GzipFile(fileobj=compressed_file)  # unzip .gz file
    output = pd.read_json(decompressed_file)
    print(output)

    return output


def create_display_campaigns_report(
    profileId_df: pd.DataFrame, ads_headers_v2: dict, date: str, i: int
):
    json_data = {
        "reportDate": date,
        "metrics": "campaignName,cost,attributedSales14d,attributedUnitsOrdered14d",
        "tactic": "T00020",
    }

    response = requests.post(
        profileId_df["url_v2_post"][i],
        headers=ads_headers_v2,
        json=json_data,
    )
    print(response)
    try:
        while response.status_code != 202:
            time.sleep(2)
            response = requests.post(
                profileId_df["url_v2_post"][i],
                headers=ads_headers_v2,
                json=json_data,
            )
            report_id = response.json()["reportId"]

            display_report_data = {
                "report_id": report_id,
                "date": date,
                "marketplace": profileId_df["marketplace"][i],
                "credentials": ads_headers_v2,
                "url": profileId_df["url_v2_get"][i],
            }

        report_id = response.json()["reportId"]

        display_report_data = {
            "report_id": report_id,
            "date": date,
            "marketplace": profileId_df["marketplace"][i],
            "credentials": ads_headers_v2,
            "url": profileId_df["url_v2_get"][i],
        }

    except Exception as Argument:
        display_report_data = {
            "report_id": str(Argument),
            "date": date,
            "marketplace": profileId_df["marketplace"][i],
            "credentials": ads_headers_v2,
            "url": profileId_df["url_v2_get"][i],
        }

    return display_report_data


def get_campaigns_report_v2(campaing_report_ids: pd.DataFrame, i: int):
    data = campaing_report_ids["credentials"][i]
    response = requests.get(
        campaing_report_ids["url"][i] + campaing_report_ids["report_id"][i],
        headers=data,
    )
    if "status" in response.json():
        while response.json()["status"] == "IN_PROGRESS":
            print("sleeping...")
            time.sleep(5)
            response = requests.get(
                campaing_report_ids["url"][i] + campaing_report_ids["report_id"][i],
                headers=data,
            )
            print(response.json())

        url = response.json()["location"]
        if url == None:
            print(response.json())

        response = requests.get(url, headers=data)
        print(response.content)
        compressed_file = io.BytesIO(response.content)  # extract .gz file
        decompressed_file = gzip.GzipFile(fileobj=compressed_file)  # unzip .gz file
        output = pd.read_json(decompressed_file)
        output["marketplace"] = campaing_report_ids["marketplace"][i]
        output["date"] = campaing_report_ids["date"][i]
        return output
    else:
        print(response.json())
        data_dict = response.json()  # suppose this is your dictionary
        output = pd.DataFrame([data_dict])
        output["date"] = campaing_report_ids["date"][i]
        output["marketplace"] = campaing_report_ids["marketplace"][i]
        return output


def create_brand_campaigns_report(
    profileId_df: pd.DataFrame, ads_headers_v2: dict, date: str, i: int
):
    json_data = {
        "reportDate": date,
        "metrics": "campaignName,cost,attributedUnitsOrdered14d,attributedSales14d,attributedConversions14d",
        "creativeType": "all",
        "segment": "placement",
    }

    response = requests.post(
        profileId_df["url_v2_post"][i].replace("/sd/", "/v2/hsa/"),
        headers=ads_headers_v2,
        json=json_data,
    )
    print(response)
    try:
        while response.status_code != 202:
            time.sleep(2)
            response = requests.post(
                profileId_df["url_v2_post"][i],
                headers=ads_headers_v2,
                json=json_data,
            )
            report_id = response.json()["reportId"]

            display_report_data = {
                "report_id": report_id,
                "date": date,
                "marketplace": profileId_df["marketplace"][i],
                "credentials": ads_headers_v2,
                "url": profileId_df["url_v2_get"][i],
            }

        report_id = response.json()["reportId"]

        display_report_data = {
            "report_id": report_id,
            "date": date,
            "marketplace": profileId_df["marketplace"][i],
            "credentials": ads_headers_v2,
            "url": profileId_df["url_v2_get"][i],
        }

    except Exception as Argument:
        display_report_data = {
            "report_id": str(Argument),
            "date": date,
            "marketplace": profileId_df["marketplace"][i],
            "credentials": ads_headers_v2,
            "url": profileId_df["url_v2_get"][i],
        }

    return display_report_data


def JoinAsinsForCampaigns(campaign_df: pd.DataFrame, productByCampaign: pd.DataFrame):
    grouped_campaign_df = campaign_df.groupby(["campaignName", "marketplace"]).agg(
        {"spend": "sum", "purchases7d": "sum", "sales7d": "sum"}
    )  # grouping by the campaign df using the campaign name and marketplace and aggregating the spend, purchases and sales

    grouped_campaign_df = grouped_campaign_df.reset_index()
    grouped_campaign_df["campaignName"] = grouped_campaign_df[
        "campaignName"
    ].str.rstrip()
    grouped_campaign_df_merged = grouped_campaign_df.merge(
        productByCampaign[
            ["campaignName", "Asin", "ValueCount", "marketplace", "Category"]
        ],
        on=["campaignName", "marketplace"],
        how="left",
    )  # merging the campaign df with the product to campaign map, adding the values count

    grouped_campaign_df_merged["real_spend"] = (
        grouped_campaign_df_merged["spend"] / grouped_campaign_df_merged["ValueCount"]
    )
    grouped_campaign_df_merged["real_sales"] = (
        grouped_campaign_df_merged["sales7d"] / grouped_campaign_df_merged["ValueCount"]
    )
    grouped_campaign_df_merged["real_ordered"] = (
        grouped_campaign_df_merged["purchases7d"]
        / grouped_campaign_df_merged["ValueCount"]
    )
    summarizeProductsAndCampaings = grouped_campaign_df_merged[
        [
            "campaignName",
            "Category",
            "marketplace",
            "Asin",
            "real_spend",
            "real_sales",
            "real_ordered",
        ]
    ]
    summarizeProductsAndCampaingsGroup = summarizeProductsAndCampaings.groupby(
        ["Asin", "marketplace"]
    ).agg({"real_spend": "sum", "real_sales": "sum", "real_ordered": "sum"})
    summarizeProductsAndCampaingsGroup = (
        summarizeProductsAndCampaingsGroup.reset_index()
    )
    summarizeProductsAndCampaingsGroup = summarizeProductsAndCampaingsGroup.merge(
        summarizeProductsAndCampaings[["Category", "marketplace", "Asin"]],
        on=["marketplace", "Asin"],
        how="left",
    )
    summarizeProductsAndCampaingsGroup = (
        summarizeProductsAndCampaingsGroup.drop_duplicates()
    )
    return summarizeProductsAndCampaingsGroup


def request_report_generation(
    credentials_df: pd.DataFrame,
    profileId_df: pd.DataFrame,
    productByCampaign: pd.DataFrame,
    startDate: str,
    endDate: str,
):
    for key in st.session_state.keys():
        del st.session_state[key]

    progress_text = (
        "Operation in progress. Please wait."  # define the progress bar text
    )
    progress = 0
    _my_bar = st.progress(
        progress, text=progress_text
    )  # create a progress bar starting from 0 presenting the progress text

    productByCampaign = (
        productByCampaign.drop_duplicates()
    )  # dropping duplicates in the product_campagin df

    profileId_df["profile_id"] = profileId_df["profile_id"].astype(
        str
    )  # define the profile_id as strings

    profileId_df = profileId_df[
        profileId_df["marketplace"].isin(productByCampaign["marketplace"])
    ]  # Filtering the profile ids df to contain only marketplaces with campaigns

    # define the amazon ads credentials
    ADS_REFRESH_TOKEN = credentials_df["ads_api"]["ADS_REFRESH_TOKEN"]
    ADS_CLIENT_ID = credentials_df["ads_api"]["ADS_CLIENT_ID"]
    ADS_CLIENT_SECRET = credentials_df["ads_api"]["ADS_CLIENT_SECRET"]

    ads_headers_refresh = {
        "client_id": ADS_CLIENT_ID,
        "refresh_token": ADS_REFRESH_TOKEN,
        "client_secret": ADS_CLIENT_SECRET,
    }  # define the headers for the access token generation

    access_token = new_access_token(ads_headers_refresh)  # generate new access token
    startDate = startDate  # start date to be filled by GUI
    endDate = endDate  # start date to be filled by GUI

    date_list = create_date_list(
        startDate, endDate
    )  # creating a list of all the dates between the first and last date

    progress_unit = 0.9 / (
        len(date_list) * 4 * len(profileId_df) + 6 * len(profileId_df)
    )  # calculating a progress unit
    progress = 0.05
    _my_bar.progress(
        progress, text=progress_text
    )  # turning the progress bar to 5 precent

    display_reports_id = pd.DataFrame(
        columns=["date", "marketplace", "report_id", "credentials", "url"]
    )
    brand_reports_id = pd.DataFrame(
        columns=["date", "marketplace", "report_id", "credentials", "url"]
    )

    for i in profileId_df.index:
        ads_headers_v2 = {
            "Content-Type": "application/json",
            "Amazon-Advertising-API-ClientId": ADS_CLIENT_ID,
            "Authorization": "Bearer " + access_token,
            "Amazon-Advertising-API-Scope": profileId_df["profile_id"][i],
        }

        for date in date_list:
            display_campaign_report_id = create_display_campaigns_report(
                profileId_df, ads_headers_v2, date, i
            )
            display_reports_id = display_reports_id._append(
                display_campaign_report_id, ignore_index=True
            )
            brand_campaign_report_id = create_brand_campaigns_report(
                profileId_df, ads_headers_v2, date, i
            )
            brand_reports_id = brand_reports_id._append(
                brand_campaign_report_id, ignore_index=True
            )
            progress = progress + progress_unit * 2

        _my_bar.progress(progress, text=progress_text)

    product_campaing_report_ids = pd.DataFrame(
        columns=["report_id", "profile_id", "marketplace", "credentials", "url"]
    )
    for i in profileId_df.index:
        ads_headers = {
            "Content-Type": "application/vnd.createasyncreportrequest.v3+json",
            "Amazon-Advertising-API-ClientId": ADS_CLIENT_ID,
            "Authorization": "Bearer " + access_token,
            "Amazon-Advertising-API-Scope": profileId_df["profile_id"][i],
        }  # header for the report generation

        report_id = create_reportsByCampaign(
            ads_headers,
            profileId_df["url"][i],
            startDate,
            endDate,
        )  # creating a report and getting the report_id

        product_campaing_report_ids = product_campaing_report_ids._append(
            {
                "report_id": report_id,
                "profile_id": profileId_df["profile_id"][i],
                "marketplace": profileId_df["marketplace"][i],
                "credentials": ads_headers,
                "url": profileId_df["url"][i],
            },
            ignore_index=True,
        )
        progress = progress + progress_unit

        _my_bar.progress(progress, text=progress_text)

    report_id_df = pd.DataFrame(columns=["marketplace", "report_id", "credentials"])

    for i in profileId_df.index:
        report_id = generate_bussiness_report(
            credentials_df[profileId_df["credentials"][i]].to_dict(),
            startDate,
            endDate,
            profileId_df["marketplace"][i],
        )
        report_id_df = report_id_df._append(
            {
                "marketplace": profileId_df["marketplace"][i],
                "report_id": report_id,
                "credentials": profileId_df["credentials"][i],
            },
            ignore_index=True,
        )
        progress = progress + progress_unit

        _my_bar.progress(progress, text=progress_text)

    st.session_state["progress"] = progress
    st.session_state["sponsered_products_report_ids"] = product_campaing_report_ids
    st.session_state["sponsered_display_report_ids"] = display_reports_id
    st.session_state["sponsered_brand_report_ids"] = brand_reports_id
    st.session_state["bussiness_report_ids"] = report_id_df
    # st.dataframe(st.session_state["sponsered_products_report_ids"])
    # st.dataframe(st.session_state["sponsered_display_report_ids"])
    # st.dataframe(st.session_state["sponsered_brand_report_ids"])
    # st.dataframe(st.session_state["bussiness_report_ids"])

    product_campaing_report_ids[
        "got_report"
    ] = ""  # creating an empty column to test the reports gotten
    return _my_bar, progress_text, progress_unit


def pull_reports_generate_report(
    product_campaing_report_ids: pd.DataFrame,
    brand_reports_id: pd.DataFrame,
    display_reports_id: pd.DataFrame,
    report_id_df: pd.DataFrame,
    _my_bar,
    progress_text,
    progress_unit,
    productByCampaign,
    credentials_df,
    profit_df,
):
    full_product_campaign_report = pd.DataFrame()
    progress = st.session_state["progress"]
    _my_bar.progress(progress, text=progress_text)

    for i in product_campaing_report_ids.index:
        product_campagin_data_df = get_reportByCampaign(
            product_campaing_report_ids["credentials"][i],
            product_campaing_report_ids["report_id"][i],
            product_campaing_report_ids["url"][i],
        )  # getting the report for the campaign preformence for every marketplace

        product_campagin_data_df["marketplace"] = product_campaing_report_ids[
            "marketplace"
        ][
            i
        ]  # adding a column with the market place to the campaigns

        if (
            full_product_campaign_report is None
        ):  # if there is no df named full_campaign_report
            full_product_campaign_report = product_campagin_data_df  # create one with the first full bussiness report
        else:
            full_product_campaign_report = full_product_campaign_report._append(
                product_campagin_data_df
            )  # append the bussiness report to the full_bussiness_report df
        progress = progress + progress_unit

        _my_bar.progress(progress, text=progress_text)
        product_campaing_report_ids["got_report"][i] = product_campaing_report_ids[
            "marketplace"
        ][i]
    product_campaigns_full_report = full_product_campaign_report

    _my_bar.progress(progress, text=progress_text)
    full_campaign_report = pd.DataFrame()
    full_brand_report = pd.DataFrame()
    brand_reports_id["got_report"] = ""
    display_reports_id[
        "got_report"
    ] = ""  # creating an empty column to test the reports gotten

    for i in display_reports_id.index:
        campagin_data_df = get_campaigns_report_v2(
            display_reports_id, i
        )  # getting the report for the campaign preformence for every marketplace

        if full_campaign_report is None:  # if there is no df named full_campaign_report
            full_campaign_report = (
                campagin_data_df  # create one with the first full bussiness report
            )
        else:
            full_campaign_report = full_campaign_report._append(
                campagin_data_df
            )  # append the bussiness report to the full_bussiness_report df
        display_reports_id["got_report"][i] = display_reports_id["marketplace"][i]
        progress = progress + progress_unit
        _my_bar.progress(progress, text=progress_text)
    display_campaign_full_report = full_campaign_report
    for i in brand_reports_id.index:
        brand_campagin_data_df = get_campaigns_report_v2(
            brand_reports_id, i
        )  # getting the report for the campaign preformence for every marketplace

        if full_brand_report is None:  # if there is no df named full_campaign_report
            full_brand_report = brand_campagin_data_df  # create one with the first full bussiness report
        else:
            full_brand_report = full_brand_report._append(
                brand_campagin_data_df
            )  # append the bussiness report to the full_bussiness_report df
        brand_reports_id["got_report"][i] = brand_reports_id["marketplace"][i]
        progress = progress + progress_unit
        _my_bar.progress(progress, text=progress_text)
    brand_campaign_full_report = full_brand_report

    display_df = display_campaign_full_report.rename(
        columns={
            "cost": "spend",
            "attributedUnitsOrdered14d": "purchases7d",
            "attributedSales14d": "sales7d",
        }
    )  # renaming the columns of the display campaing to match the other reports

    brand_df = brand_campaign_full_report.rename(
        columns={
            "cost": "spend",
            "attributedConversions14d": "purchases7d",
            "attributedSales14d": "sales7d",
        }
    )  # renaming the columns of the display campaing  to match the other reports
    brand_df.to_excel("./brand_campaigns.xlsx")
    product_campaigns_full_report.to_excel("./product_campaigns.xlsx")
    campaign_df = product_campaigns_full_report._append(display_df)._append(
        brand_df
    )  # appending all the campaign reports together to a single df

    summarizeProductsAndCampaingsGroup = JoinAsinsForCampaigns(
        campaign_df, productByCampaign
    )  # Join the Asins of the products to the campaigns data
    full_business_report = pd.DataFrame()
    report_id_df["got_report"] = ""
    for i in report_id_df.index:
        business_report = get_bussiness_report(
            credentials_df[report_id_df["credentials"][i]].to_dict(),
            report_id_df["report_id"][i],
            report_id_df["marketplace"][i],
        )

        if full_business_report is None:  # if there is no df named full_campaign_report
            full_business_report = (
                business_report  # create one with the first full bussiness report
            )
        else:
            full_business_report = full_business_report._append(
                business_report
            )  # append the bussiness report to the full_bussiness_report df
        report_id_df["got_report"][i] = report_id_df["marketplace"][i]
        progress = progress + progress_unit

        _my_bar.progress(progress, text=progress_text)

    _my_bar.progress(0.95, progress_text)  # setting the progress bar to 95 precent

    summarizeProductsAndCampaingsGroup["Asin"] = summarizeProductsAndCampaingsGroup[
        "Asin"
    ].str.rstrip()  # striping extra white space on the Asin column

    full_business_report["childAsin"] = full_business_report["childAsin"].str.rstrip()
    full_business_report.to_excel("./Bussiness_report.xlsx")
    summarizeProductsAndCampaingsWithReport = summarizeProductsAndCampaingsGroup.merge(
        full_business_report[
            [
                "unitsOrdered",
                "childAsin",
                "unitSessionPercentage",
                "amount",
                "marketplace",
            ]
        ],
        left_on=["Asin", "marketplace"],
        right_on=["childAsin", "marketplace"],
        how="left",
    )  # joining the ppc report with the  business report on marketplace and Asin

    summarizeProductsAndCampaingsWithReport["UnitPrice"] = (
        summarizeProductsAndCampaingsWithReport["amount"]
        / summarizeProductsAndCampaingsWithReport["unitsOrdered"]
    )  # calculating the price of one unit

    # convert string to date object
    startDatetime = datetime.strptime(startDate, "%Y-%m-%d")
    endDatetime = datetime.strptime(endDate, "%Y-%m-%d")

    # difference between dates in timedelta
    delta = endDatetime - startDatetime

    summarizeProductsAndCampaingsWithReport["period"] = delta / pd.Timedelta(
        days=1
    )  # the period of the report in days

    summarizeProductsAndCampaingsWithReport["period"] = (
        summarizeProductsAndCampaingsWithReport["period"] + 1
    )
    # the period of the report in days

    summarizeProductsAndCampaingsWithReport["DailySales"] = (
        summarizeProductsAndCampaingsWithReport["unitsOrdered"]
        / summarizeProductsAndCampaingsWithReport["period"]
    )  # calculating the units ordered per day

    summarizeProductsAndCampaingsWithReport["ppcCostPerUnit"] = (
        summarizeProductsAndCampaingsWithReport["real_spend"]
        / summarizeProductsAndCampaingsWithReport["real_ordered"]
    )  # calculating the ppc cost per unit

    summarizeProductsAndCampaingsWithReport["Acos"] = (
        summarizeProductsAndCampaingsWithReport["real_spend"]
        / summarizeProductsAndCampaingsWithReport["real_sales"]
    )  # calculating the Acos

    summarizeProductsAndCampaingsWithReport[
        "date"
    ] = datetime.today()  # filling the date column wwith todays date

    summarizeProductsAndCampaingsWithReport = summarizeProductsAndCampaingsWithReport[
        [
            "marketplace",
            "Category",
            "Asin",
            "date",
            "period",
            "real_spend",
            "real_sales",
            "amount",
            "real_ordered",
            "unitsOrdered",
            "DailySales",
            "unitSessionPercentage",
            "ppcCostPerUnit",
            "UnitPrice",
            "Acos",
        ]
    ]  # filtering the df for importanet columns
    summarizeProductsAndCampaingsWithReport = (
        summarizeProductsAndCampaingsWithReport.sort_values(by="marketplace")
    )

    summarizeProductsAndCampaingsWithReport = (
        summarizeProductsAndCampaingsWithReport.rename(
            columns={
                "Acos": "Avg Acos %",
                "date": "Check Date",
                "period": "Period",
                "real_spend": "Campaign Spend",
                "UnitPrice": "Sale Price",
                "real_ordered": "Unit Sales From PPC",
                "DailySales": "Unit Daily Sales",
                "ppcCostPerUnit": "PPC Cost Without Organic Sales",
                "unitsOrdered": "Unit Sales Total",
                "unitSessionPercentage": "Unit Session %",
                "amount": "Total Sales",
            }
        )
    )
    # renaming the columns of the df
    summarizeProductsAndCampaingsWithReport[
        "Check Date"
    ] = summarizeProductsAndCampaingsWithReport["Check Date"].dt.date
    summarizeProductsAndCampaingsWithReport["Start Date"] = startDatetime
    summarizeProductsAndCampaingsWithReport[
        "Start Date"
    ] = summarizeProductsAndCampaingsWithReport["Start Date"].dt.date

    summarizeProductsAndCampaingsWithReport["End Date"] = endDatetime
    summarizeProductsAndCampaingsWithReport[
        "End Date"
    ] = summarizeProductsAndCampaingsWithReport["End Date"].dt.date
    summarizeProductsAndCampaingsWithReport["Total PPC Cost Per Unit"] = (
        summarizeProductsAndCampaingsWithReport["Campaign Spend"]
        / summarizeProductsAndCampaingsWithReport["Unit Sales Total"]
    )
    summarizeProductsAndCampaingsWithReport["Rating"] = ""
    summarizeProductsAndCampaingsWithReport["Notes"] = ""

    profit_df["ASIN"] = profit_df[
        "ASIN"
    ].str.rstrip()  # right striping the series for extra spaces

    summarizeProductsAndCampaingsWithReport = (
        summarizeProductsAndCampaingsWithReport.merge(
            profit_df[["profit without PPC", "ASIN", "Country",'Currency conversion ILS']],
            left_on=["marketplace", "Asin"],
            right_on=["Country", "ASIN"],
            how="left",
        )  # merging the profit df with the full report based on asin and marketplace
    )

    summarizeProductsAndCampaingsWithReport[
        "Profit Per Unit After PPC ILS"
    ] = (summarizeProductsAndCampaingsWithReport[
        "profit without PPC"
    ] - (summarizeProductsAndCampaingsWithReport[
        "Total PPC Cost Per Unit"
    ])*(summarizeProductsAndCampaingsWithReport[
        "Currency conversion ILS"
    ])).fillna(
        0
    )  # calculating the profit per unit

    summarizeProductsAndCampaingsWithReport["Profit 30 Days ILS"] = (
        summarizeProductsAndCampaingsWithReport["Profit Per Unit After PPC ILS"]
        * (
            summarizeProductsAndCampaingsWithReport["Unit Sales From PPC"]
            / summarizeProductsAndCampaingsWithReport["Period"]
        )
    ) * 30  # calculating the profit revenue(monthly revenue)

    summarizeProductsAndCampaingsWithReport = summarizeProductsAndCampaingsWithReport[
        [
            "marketplace",
            "Category",
            "Asin",
            "Check Date",
            "Start Date",
            "End Date",
            "Period",
            "Unit Sales Total",
            "Unit Daily Sales",
            "Unit Session %",
            "Sale Price",
            "Rating",
            "Unit Sales From PPC",
            "Campaign Spend",
            "PPC Cost Without Organic Sales",
            "Total PPC Cost Per Unit",
            "Avg Acos %",
            "Profit Per Unit After PPC ILS",
            "Profit 30 Days ILS",
            "Total Sales",
            "profit without PPC",
            "Notes",
        ]
    ]
    profit_df = profit_df[
        [
            "Country",
            "ASIN",
            "Tyroler Code",
            "Product",
            "Price",
            "Currency conversion ILS",
            "amazon shipment fee",
            "product cost",
            "profit without PPC",
            "Shipment cost to amazon",
        ]
    ].merge(
        summarizeProductsAndCampaingsWithReport[
            ["Asin", "marketplace", "Total PPC Cost Per Unit"]
        ],
        how="left",
        left_on=["Country", "ASIN"],
        right_on=["marketplace", "Asin"],
    )  # merging the profit df and the full report on country and asin

    profit_df["Total PPC Cost Per Unit"] = profit_df["Total PPC Cost Per Unit"].fillna(
        0
    )  # filling ppc cost without organic sales na's with 0

    profit_df["profit after ppc"] = (
        profit_df["profit without PPC"] - profit_df["Total PPC Cost Per Unit"]
    )  # calculating the profit after ppc
    profit_df = profit_df.drop_duplicates()  # dropping duplicates from the profit df

    _my_bar.progress(1)

    return (
        summarizeProductsAndCampaingsWithReport,
        profit_df,
        brand_df,
        display_df,
        product_campaigns_full_report,
    )


st.set_page_config(
    page_title="Tyroller PPC",
    page_icon="🧊",
)  # configuring the page layout


left_column, right_column = st.columns(2)  # creating two columns

with left_column:  # on the left columns
    st.markdown("")  # blank placeholder
    st.markdown("")  # blank placeholder

    image = Image.open("./cropped-tyroler-logo.png")  # open the logo image
    st.image(image, use_column_width=True)  # present the logo image

with right_column:  # on the right columns
    st.header("Amazon PPC Reports Automation")  # write an header

st.markdown("---")  # create a sperator  line

left_column2, right_column2 = st.columns(2)  # create two bottom columns

with left_column2:  # on the left bottom column
    credentials = st.file_uploader("Keys:")  # create a file uploader for the keys json
    if credentials is not None:  # if there is a file uploaded
        credentials = pd.read_json(credentials)  # read the keys json into df

    ProductByCampaign = st.file_uploader(
        "Product by campaign:"
    )  # create a file uploader for the product campagins info
    if ProductByCampaign is not None:  # if there is a file uploaded
        ProductByCampaign_df = pd.read_excel(
            ProductByCampaign
        )  # read the product campagins info into df

with right_column2:  # on the bottom right columns
    profile_id = st.file_uploader(
        "Profile Ids:"
    )  # create a file uploader for the profile ids data
    if profile_id is not None:  # if there is a file uploaded
        profile_id_df = pd.read_excel(
            profile_id, sheet_name="profile_id"
        )  # read the profile_id sheet as df

    profit = st.file_uploader(
        "Profit Table:"
    )  # create a file uploader for the profits data
    if profit is not None:  # if there is a file uploaded
        profit_df = pd.read_excel(profit)  # read the profit file into a df

    endDate = st.text_input(
        label="End Date:(yyyy-mm-dd)"
    )  # a text input slot for the end date

with left_column2:
    startDate = st.text_input(
        label="Start Date:(yyyy-mm-dd)"
    )  # a text input slot for the start date


run = st.button(
    "Request Reports", type="primary"
)  # creating a primary button to run the report generation function
# if run:  # if the button is pressed
#     final_report, profit_final = generate_ppc_report(
#         credentials, profile_id_df, ProductByCampaign_df, profit_df, startDate, endDate
#     )  # generate the ppc and profit report
if run:
    (
        st.session_state["_my_bar"],
        st.session_state["bar_text"],
        st.session_state["bar_unit"],
    ) = request_report_generation(
        credentials, profile_id_df, ProductByCampaign_df, startDate, endDate
    )
generate = st.button("Generate final report")
if generate:
    (
        final_report,
        profit_final,
        brand_report,
        display_report,
        product_report,
    ) = pull_reports_generate_report(
        st.session_state["sponsered_products_report_ids"],
        st.session_state["sponsered_brand_report_ids"],
        st.session_state["sponsered_display_report_ids"],
        st.session_state["bussiness_report_ids"],
        st.session_state["_my_bar"],
        st.session_state["bar_text"],
        st.session_state["bar_unit"],
        ProductByCampaign_df,
        credentials,
        profit_df,
    )

    def dataframe_to_excel_byte(df: pd.DataFrame, sheetName: str):
        xlsx_data = BytesIO()  # create an empty bytes object
        with pd.ExcelWriter(
            xlsx_data, engine="openpyxl"
        ) as writer:  # define the excel writer as writer
            df.to_excel(
                writer, sheet_name=sheetName
            )  # write the final report into byte object
        xlsx_data.seek(0)
        return xlsx_data  # run the byte to the beggining

    xlsx_data = dataframe_to_excel_byte(final_report, "PPC Report")
    profit_xlsx_data = dataframe_to_excel_byte(profit_final, "Profit Report")
    brand_xlsx_data = dataframe_to_excel_byte(brand_report, "Brand Report")
    display_xlsx_data = dataframe_to_excel_byte(display_report, "Display Report")
    product_xlsx_data = dataframe_to_excel_byte(product_report, "Product Report")

    zip_buffer = io.BytesIO()  # create empty byte for zip
    with zipfile.ZipFile(
        zip_buffer, mode="w", compression=zipfile.ZIP_DEFLATED
    ) as zf:  # write files to the zip buffer
        # Add each file to the zip file
        zf.writestr(
            "PPC Report.xlsx", xlsx_data.read()
        )  # read the content of the xlsx_data
        zf.writestr(
            "Profit Report.xlsx", profit_xlsx_data.read()
        )  # read the content of the profit_xlsx_data
        zf.writestr(
            "Brand Report.xlsx", brand_xlsx_data.read()
        )  # read the content of the profit_xlsx_data
        zf.writestr(
            "Display Report.xlsx", display_xlsx_data.read()
        )  # read the content of the profit_xlsx_data
        zf.writestr(
            "Product Report.xlsx", product_xlsx_data.read()
        )  # read the content of the profit_xlsx_data
    zip_buffer.seek(0)  # Reset the file pointer to the beginning

    st.session_state["buffer"] = zip_buffer
    st.success("הניטור הושלם בהצלחה")
try:
    download = st.download_button(
        label="📥 Download Reports",
        data=st.session_state["buffer"],
        file_name="Reports.zip",
        key="download_button",
    )
except KeyError:
    st.write(
        "a Downlaod button will appear once the report is ready"
    )  # a download button for the zip file
