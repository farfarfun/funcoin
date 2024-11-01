from datetime import datetime, timedelta


def year_first_datetime(years):
    today = datetime.today()
    return datetime(today.year + years, 1, 1)


def month_first_datetime(months):
    today = datetime.today()
    months = today.month + months - 1
    years = -int((months % 12 - months) / 12)
    months = months % 12 + 1
    return datetime(today.year + years, months, 1)


def year_during(years):
    first = year_first_datetime(years)
    last = year_first_datetime(years + 1) - timedelta(seconds=1)
    return first, last


def month_during(months):
    first = month_first_datetime(months)
    last = month_first_datetime(months + 1) - timedelta(seconds=1)
    return first, last


def week_during(weeks):
    today = datetime.today()
    first = today + timedelta(weeks=weeks) - timedelta(days=today.weekday())
    first = datetime(first.year, first.month, first.day)
    last = first + timedelta(weeks=1) - timedelta(seconds=1)
    return first, last


def day_during(days):
    today = datetime.today()
    first = today + timedelta(days=days)
    first = datetime(first.year, first.month, first.day)
    last = first + timedelta(days=1) - timedelta(seconds=1)
    return first, last
