import pandas as pd
import numpy as np
import queue
import math
import random

GEMEENTENAMEN_CORRECTIONS = {
    'Groningen (gemeente)': 'Groningen',
    'Hengelo (O.)': 'Hengelo',
    'Utrecht (gemeente)': 'Utrecht',
    'Laren (NH.)': 'Laren',
    "'s-Gravenhage (gemeente)": "'s-Gravenhage",
    'Rijswijk (ZH.)': 'Rijswijk',
    'Middelburg (Z.)': 'Middelburg',
    'Beek (L.)': 'Beek',
    'Stein (L.)': 'Stein',
}

GROUPED_MUNICIPALITIES = {
    'waadhoeke': ['Franekeradeel', 'het Bildt', 'Menameradiel', 'Littenseradiel'],
    'westerwolde': ['Bellingwedde', 'Vlagtwedde'],
    'midden_groningen': ['Hoogezand-Sappemeer', 'Slochteren', 'Menterwolde'],
    'beekdaelen': ['Onderbanken', 'Nuth', 'Schinnen'],
    'altena': ['Aalburg', 'Werkendam', 'Woudrichem'],
    'west_betuwe': ['Geldermalsen', 'Neerijnen', 'Lingewaal'],
    'vijfheerenlanden': ['Leerdam', 'Vianen', 'Zederik'],
    'hoeksche_waard': ['Binnenmaas', 'Cromstrijen', 'Korendijk', 'Oud-Beijerland', 'Strijen'],
    'hogeland': ['Bedum', 'Eemsmond', 'De Marne', 'Winsum'],
    'westerkwartier': ['Grootegast', 'Leek', 'Marum', 'Zuidhorn', 'Winsum'],
    'no_friesland': ['Dongeradeel', 'Ferwerderadeel', 'Kollumerland', 'Nieuwkruisland'],
    'molenlanden': ['Molenwaard', 'Giessenlanden']
}


def implement_gemeentenamen_corrections(df: pd.DataFrame, corrections: 'dict[str, str]' = GEMEENTENAMEN_CORRECTIONS) -> pd.DataFrame:
    for key in corrections:
        df = df.replace(key, corrections[key])
    return df


def standardize_units_in_column(column: pd.Series) -> pd.Series:
    column = column.str.replace(',', '.')
    column = column.astype(float)
    return column


def add_grouped_municipalities_data(data: pd.DataFrame, column_name: str, grouped_municipalities: 'dict[str, list[str]]' = GROUPED_MUNICIPALITIES) -> pd.DataFrame:
    MUNICIPALITY_NAMES_COLUMN = '\xa0'
    for municipality in grouped_municipalities:
        municipalities_df = data[data[MUNICIPALITY_NAMES_COLUMN].isin(
            grouped_municipalities[municipality])]
        municipalities_df = municipalities_df.replace(',', '.')
        municipalities_df[column_name] = standardize_units_in_column(
            municipalities_df[column_name])
        municipalities_df = pd.DataFrame({MUNICIPALITY_NAMES_COLUMN: municipality, column_name: str(
            municipalities_df[column_name].mean())}, index=[0])
        data = pd.concat(
            [data, municipalities_df], ignore_index=True)
    return data


# buurten = pd.read_csv('buurten_nederland.csv')
gemeente_data = pd.read_csv(
    'Huishoudens__samenstelling__regio_28062022_103941.csv', sep=';')
gemeente_data = gemeente_data.dropna()
gemeente_data = implement_gemeentenamen_corrections(
    gemeente_data)

######

average_m2_per_one_person_hh_per_municipality = pd.read_csv(
    'average_m2_per_one_person_hh_per_municipality.csv')
average_m2_per_one_person_hh_per_municipality = implement_gemeentenamen_corrections(
    average_m2_per_one_person_hh_per_municipality)
average_m2_per_one_person_hh_per_municipality = add_grouped_municipalities_data(
    average_m2_per_one_person_hh_per_municipality, 'Opp. per persoon, Eenpersoonshuishoudens ( m2)')

average_m2_per_person_in_couples_with_kids_per_municipality = pd.read_csv(
    'average_m2_per_person_in_couples_with_kids_per_municipality.csv')
average_m2_per_person_in_couples_with_kids_per_municipality = implement_gemeentenamen_corrections(
    average_m2_per_person_in_couples_with_kids_per_municipality)
average_m2_per_person_in_couples_with_kids_per_municipality = add_grouped_municipalities_data(
    average_m2_per_person_in_couples_with_kids_per_municipality, 'Opp. per persoon, Paren met kinderen ( m2)')

average_m2_per_person_in_couples_without_kids_per_municipality = pd.read_csv(
    'average_m2_per_person_in_couples_without_kids_per_municipality.csv')
average_m2_per_person_in_couples_without_kids_per_municipality = implement_gemeentenamen_corrections(
    average_m2_per_person_in_couples_without_kids_per_municipality)
average_m2_per_person_in_couples_without_kids_per_municipality = add_grouped_municipalities_data(
    average_m2_per_person_in_couples_without_kids_per_municipality, 'Opp. per persoon,  Paren zonder kinderen ( m2)')

average_m2_per_person_per_municipality = pd.read_csv(
    'average_m2_per_person_per_municipality.csv')
average_m2_per_person_per_municipality = implement_gemeentenamen_corrections(
    average_m2_per_person_per_municipality)
average_m2_per_person_per_municipality = add_grouped_municipalities_data(
    average_m2_per_person_per_municipality, 'Opp. per persoon, Alle huishoudens ( m2)')

######

leeftijden_per_huishouden = pd.read_csv(
    'leeftijden__per__gemeente.csv', sep=';')
leeftijden_per_huishouden = leeftijden_per_huishouden.fillna(0)

######

dtypes = {'bu_code': str, 'identificatie': str,
          'oppervlakteverblijfsobject': int, 'gebruiksdoelverblijfsobject': str}
buildings = pd.read_csv('verblijfsobjecten_nederland.csv', dtype=dtypes)

######

summed = leeftijden_per_huishouden.groupby('Leeftijd').sum()

data_0_14 = summed.iloc[0:3].sum()
data_15_24 = summed.iloc[3:5].sum()
data_25_44 = summed.iloc[5:9].sum()
data_45_64 = summed.iloc[9:13].sum()
data_65_plus = summed.iloc[13:].sum()

######
########################################################
######


def getAgeGroupsPerMunicipality(municipalities, municipality):

    data = municipalities[municipalities['RegioS'] == municipality]

    data_0_14 = data.iloc[0:3].groupby('Perioden').sum().reset_index().iloc[0]
    data_15_24 = data.iloc[3:5].groupby('Perioden').sum().reset_index().iloc[0]
    data_25_44 = data.iloc[5:9].groupby('Perioden').sum().reset_index().iloc[0]
    data_45_64 = data.iloc[9:13].groupby(
        'Perioden').sum().reset_index().iloc[0]
    data_65_plus = data.iloc[13:].groupby(
        'Perioden').sum().reset_index().iloc[0]

    return {'0-14': data_0_14, '15-24': data_15_24, '25-44': data_25_44, '45-64': data_45_64, '65plus': data_65_plus}


age_groups = getAgeGroupsPerMunicipality(leeftijden_per_huishouden, 'GM0505')


def getChildrenMultipliersPerMunicipality(children):

    children_multipliers = [
        1 / max(children) * children[0],
        1 / max(children) * children[1],
        1 / max(children) * children[2],
        1 / max(children) * children[3]]
    return children_multipliers


def getStatisticsForAgeGroup(age_group):
    total = age_group['TotaalInParticuliereHuishoudens_2']
    children = age_group['ThuiswonendKind_3']
    singles = age_group['Alleenstaand_4']
    total_living_together = age_group['TotaalSamenwonendePersonen_5']
    non_married_partner_without_children = age_group['PartnerInNietGehuwdPaarZonderKi_6']
    married_partner_without_children = age_group['PartnerInGehuwdPaarZonderKinderen_7']
    non_married_partner_with_children = age_group['PartnerInNietGehuwdPaarMetKinderen_8']
    married_partner_with_children = age_group['PartnerInGehuwdPaarMetKinderen_9']
    single_parents = age_group['OuderInEenouderhuishouden_10']
    other_household_members = age_group['OverigLidHuishouden_11']

    return {
        'total': total,
        'children': children,
        'singles': singles,
        'total living together': total_living_together,
        'non married without children': non_married_partner_without_children,
        'married without children': married_partner_without_children,
        'non married with children': non_married_partner_with_children,
        'married with children': married_partner_with_children,
        'single parents': single_parents,
        'other household members': other_household_members
    }


def calculateHousholdDistributionPercentages(age_group_data):
    """Returns distribution of following values as percentages: 
      - 1 person households
      - total households without children
      - households without children with couples that are not married
      - households without children with couples that are married
      - total households with children
      - households with children with couples that are not married
      - households with children with couples that are married
      - children
      - single parents

      total persons = children + single + total households without children + total households with children + single parent

      These values are based on the total persons in the age group that is being used in the calculation. E.g. of the total amount of persons in this age group, n % is a 1 person household
    """

    p_singles = round(np.nan_to_num(
        100 / age_group_data['total'] * age_group_data['singles']))

    p_households_without_children = round(np.nan_to_num(100 / age_group_data['total'] * (
        age_group_data['non married without children'] + age_group_data['married without children'])))
    p_non_married_without_children = round(np.nan_to_num(
        100 / (age_group_data['non married without children'] + age_group_data['married without children']) * age_group_data['non married without children']))
    p_married_without_children = round(np.nan_to_num(
        100 / (age_group_data['non married without children'] + age_group_data['married without children']) * age_group_data['married without children']))

    p_households_with_children = round(np.nan_to_num(100 / age_group_data['total'] * (
        age_group_data['non married with children'] + age_group_data['married with children'] + age_group_data['single parents'])))
    p_non_married_with_children = round(np.nan_to_num(
        100 / (age_group_data['non married with children'] + age_group_data['married with children']) * age_group_data['non married with children']))
    p_married_with_children = round(np.nan_to_num(
        100 / (age_group_data['non married with children'] + age_group_data['married with children']) * age_group_data['married with children']))

    p_children = round(np.nan_to_num(100 / age_group_data['total'] * (
        age_group_data['children'] + age_group_data['other household members'])))

    p_single_parents = round(np.nan_to_num(
        100 / age_group_data['total'] * age_group_data['single parents']))

    return {
        'children': p_children,
        'singles': p_singles,
        'total households without children': p_households_without_children,
        'non married without children': p_non_married_without_children,
        'married without children': p_married_without_children,
        'total households with children': p_households_with_children,
        'non married with children': p_non_married_with_children,
        'married with children': p_married_with_children,
        'single parents': p_single_parents
    }


def householdsDistributionPercentagesToValues(age_group_total, age_group_percentages):
    """Returns actual numbers of the household distribution percentages
    -------------------------------------
      - Total 1 person households
    -------------------------------------
      - Total housholds without children
      - Sub Total non married couples without children
      - Sub Total married couples without children
    -------------------------------------
      - Total households with children
      - Sub Total non married couples with children
      - Sub Total married couples with children
    -------------------------------------
      - Total children in households  
    -------------------------------------

        total persons = children + single + total households without children + total households with children + single parent

    """

    total_singles = round(age_group_total / 100 *
                          age_group_percentages['singles'])

    total_hh_wo_children = round(
        age_group_total / 100 * age_group_percentages['total households without children'])
    total_non_married_wo_children = round(
        total_hh_wo_children / 100 * age_group_percentages['non married without children'])
    total_married_wo_children = round(
        total_hh_wo_children / 100 * age_group_percentages['married without children'])

    total_hh_w_children = round(
        age_group_total / 100 * age_group_percentages['total households with children'])
    total_non_married_w_children = round(
        total_hh_w_children / 100 * age_group_percentages['non married with children'])
    total_married_w_children = round(
        total_hh_w_children / 100 * age_group_percentages['married with children'])

    total_children = round(age_group_total / 100 *
                           age_group_percentages['children'])

    total_single_parents = round(
        age_group_total / 100 * age_group_percentages['single parents'])

    return {
        'children': total_children,
        'singles': total_singles,
        'total households without children': total_hh_wo_children,
        'non married without children': total_non_married_wo_children,
        'married without children': total_married_wo_children,
        'total households with children': total_hh_w_children,
        'non married with children': total_non_married_w_children,
        'married with children': total_married_w_children,
        'single parents': total_single_parents
    }


def getHousholdSizePercentagesPerMunicipality(data, municipality):
    municipality_data = data[data['Regio\'s'] == municipality].iloc[0]

    p_eenouder = round(100 / municipality_data['Particuliere huishoudens: samenstelling/Meerpersoonshuishouden/Totaal meerpersoonshuishoudens (aantal)']
                       * municipality_data['Particuliere huishoudens: samenstelling/Meerpersoonshuishouden/Eenouderhuishouden/Totaal eenouderhuishoudens (aantal)'])
    p_2p_hh = round(100 / municipality_data['Particuliere huishoudens: samenstelling/Meerpersoonshuishouden/Totaal meerpersoonshuishoudens (aantal)']
                    * municipality_data['Particuliere huishoudens: grootte/Meerpersoonshuishouden/2 personen (aantal)'])
    p_3p_hh = round(100 / municipality_data['Particuliere huishoudens: samenstelling/Meerpersoonshuishouden/Totaal meerpersoonshuishoudens (aantal)']
                    * municipality_data['Particuliere huishoudens: grootte/Meerpersoonshuishouden/3 personen (aantal)'])
    p_4p_hh = round(100 / municipality_data['Particuliere huishoudens: samenstelling/Meerpersoonshuishouden/Totaal meerpersoonshuishoudens (aantal)']
                    * municipality_data['Particuliere huishoudens: grootte/Meerpersoonshuishouden/4 personen (aantal)'])
    p_5p_plus_hh = round(100 / municipality_data['Particuliere huishoudens: samenstelling/Meerpersoonshuishouden/Totaal meerpersoonshuishoudens (aantal)']
                         * municipality_data['Particuliere huishoudens: grootte/Meerpersoonshuishouden/5 of meer personen (aantal)'])

    return {'single parents': p_eenouder, 'two person': p_2p_hh, 'three person': p_3p_hh, 'four person': p_4p_hh, 'five person': p_5p_plus_hh}


def getAmountOfHouseholdsPerSize(percentages, amount_1p_hh, total_hh):

    aantal_2_p = round(((total_hh - amount_1p_hh) / 100)
                       * percentages['two person'])
    aantal_3_p = round(((total_hh - amount_1p_hh) / 100)
                       * percentages['three person'])
    aantal_4_p = round(((total_hh - amount_1p_hh) / 100)
                       * percentages['four person'])
    aantal_5_p_plus = round(
        ((total_hh - amount_1p_hh) / 100) * percentages['five person'])

    return {'two person': aantal_2_p, 'three person': aantal_3_p, 'four person': aantal_4_p, 'five person': aantal_5_p_plus}


def getAmountOfSingleParents(percentages, amount_of_hh_with_children):
    return round(amount_of_hh_with_children / 100 * percentages['single parents'])


def generateHouseholdSizes(amount_1p_hh, hh_per_size, single_parents, huishoudens):

    eenouder, amount_2p_hh, amount_3p_hh, amount_4p_hh, amount_5p_hh = hh_per_size

    huishouden_lijst = []
    for x in range(0, huishoudens):
        if x <= amount_1p_hh:
            huishouden_lijst.append([1, 0, 1, 'single'])

        elif x > amount_1p_hh and x <= amount_1p_hh + single_parents:
            huishouden_lijst.append([1, 1, 2, 'single parent'])

        elif x > amount_1p_hh and x <= amount_1p_hh + amount_2p_hh:
            huishouden_lijst.append([2, 0, 2, 'partners without children'])

        elif x > amount_1p_hh + amount_2p_hh and x <= amount_1p_hh + amount_2p_hh + amount_3p_hh:
            huishouden_lijst.append([2, 1, 3, 'partners with children'])

        elif x > amount_1p_hh + amount_2p_hh + amount_3p_hh and x <= amount_1p_hh + amount_2p_hh + amount_3p_hh + amount_4p_hh:
            huishouden_lijst.append([2, 2, 4, 'partners with children'])

        elif x > amount_1p_hh + amount_2p_hh + amount_3p_hh + amount_4p_hh:
            huishouden_lijst.append([2, 3, 5, 'partners with children'])

    return huishouden_lijst


def adjust_children(children_values, difference_with_required, multipliers):

    if difference_with_required == 0 or math.isnan(sum(multipliers)):
        return children_values

    new_children_values = []

    leftover_difference = 0
    # print('children values', children_values)
    # print('difference', difference_with_required)
    # print('multipliers', multipliers)

    # age group 0 - 14 are 100% of the time children so this value can't change
    new_children_values.append(children_values[0])

    ag_15_24 = children_values[1]
    ag_25_44 = children_values[2]
    ag_45_64 = children_values[3]
    ag_65_plus = children_values[4]

    age_group = [
        ag_15_24,
        ag_25_44,
        ag_45_64,
        ag_65_plus
    ]

    increment_amount = round(difference_with_required / sum(multipliers))

    used_multiplier = 0

    for x in age_group:
        n = x + round(increment_amount * multipliers[used_multiplier])

        if n < 0:
            leftover_difference += n
            new_children_values.append(0)
        else:
            new_children_values.append(n)

        used_multiplier += 1

    if leftover_difference == 0 or sum(new_children_values) == children_values[0]:
        return new_children_values
    else:
        return adjust_children(new_children_values, leftover_difference, multipliers)


def adjust_household_distribution(values, multipliers, difference_with_required):

    if difference_with_required == 0 or math.isnan(sum(multipliers)):
        return values

    new_values = []

    leftover_difference = 0

    increment_amount = round(difference_with_required / sum(multipliers))

    used_multiplier = 0

    for x in values:
        n = x + round(increment_amount * multipliers[used_multiplier])

        if n < 0:
            leftover_difference += n
            new_values.append(0)
        else:
            new_values.append(n)

        used_multiplier += 1

    if leftover_difference == 0 or sum(new_values) == 0:
        return new_values
    else:
        return adjust_household_distribution(new_values, single_multipliers, leftover_difference)


def getIncrementMultipliers(values):
    multipliers = np.array([
        1 / values.max() * values[0],
        1 / values.max() * values[1],
        1 / values.max() * values[2],
        1 / values.max() * values[3],
        1 / values.max() * values[4]])
    return multipliers


def children_count_per_age_group(children):
    x1 = children.count('0-14')
    x2 = children.count('15-24')
    x3 = children.count('25-44')
    x4 = children.count('45-64')
    x5 = children.count('65plus')

    return {'0-14': x1, '15-24': x2, '25-44': x3, '45-64': x4, '65plus': x5}


# volgens bestaande bouw bouwbesluit 2003
m2_per_person_kantoorfunctie = 8
m2_per_person_bijeenkomstfunctie = (4+8+10+8) / 4
m2_per_person_celfunctie = (3+5+4) / 3
m2_per_person_gezondheidszorgfunctie = (5+8) / 2
m2_per_person_industriefunctie = 4
m2_per_person_logiesfunctie = 4
m2_per_person_onderwijsfunctie = 8
m2_per_person_sportfunctie = 8
m2_per_person_winkelfunctie = (8+16) / 2
m2_per_person_overig = 4


def assign_max_people_in_building(buildings):

    buildings = buildings.reset_index(drop=True)

    buildings_occupants = []

    for index, building in buildings.iterrows():

        if building['gebruiksdoelverblijfsobject'] == 'kantoorfunctie':
            occupants = round(
                building['oppervlakteverblijfsobject'] / m2_per_person_kantoorfunctie)

        elif building['gebruiksdoelverblijfsobject'] == 'bijeenkomstfunctie':
            occupants = round(
                building['oppervlakteverblijfsobject'] / m2_per_person_bijeenkomstfunctie)

        elif building['gebruiksdoelverblijfsobject'] == 'celfunctie':
            occupants = round(
                building['oppervlakteverblijfsobject'] / m2_per_person_celfunctie)

        elif building['gebruiksdoelverblijfsobject'] == 'gezondheidszorgfunctie':
            occupants = round(
                building['oppervlakteverblijfsobject'] / m2_per_person_gezondheidszorgfunctie)

        elif building['gebruiksdoelverblijfsobject'] == 'industriefunctie':
            occupants = round(
                building['oppervlakteverblijfsobject'] / m2_per_person_industriefunctie)

        elif building['gebruiksdoelverblijfsobject'] == 'logiesfunctie':
            occupants = round(
                building['oppervlakteverblijfsobject'] / m2_per_person_logiesfunctie)

        elif building['gebruiksdoelverblijfsobject'] == 'onderwijsfunctie':
            occupants = round(
                building['oppervlakteverblijfsobject'] / m2_per_person_onderwijsfunctie)

        elif building['gebruiksdoelverblijfsobject'] == 'sportfunctie':
            occupants = round(
                building['oppervlakteverblijfsobject'] / m2_per_person_sportfunctie)

        elif building['gebruiksdoelverblijfsobject'] == 'winkelfunctie':
            occupants = round(
                building['oppervlakteverblijfsobject'] / m2_per_person_winkelfunctie)

        elif building['gebruiksdoelverblijfsobject'] == 'overige gebruiksfunctie':
            occupants = round(
                building['oppervlakteverblijfsobject'] / m2_per_person_overig)

        parents_age_group = {'parents_age_group': 'unknown'}
        parents_count = {'parent_count': 0}
        children = {'0-14': 0, '15-24': 0, '25-44': 0, '45-64': 0, '65plus': 0}
        children_count = {'amount_of_children': 0}
        hh_size = {'hh_size': occupants}
        category = {'household_type': 'empty'}

        buildings_occupants.append(
            {**parents_age_group, **parents_count, **children, **children_count, **hh_size, **category})

    df = pd.DataFrame(buildings_occupants)

    return buildings.merge(df, left_index=True, right_index=True)


columns = [
    'aant_inw',
    'p_00_14_jr',
    'p_15_24_jr',
    'p_25_44_jr',
    'p_45_64_jr',
    'p_65_eo_jr',
    'aantal_hh',
    'p_eenp_hh',
    'p_hh_z_k',
    'p_hh_m_k',
    'gem_hh_gr'
]

incomplete_buurten = buurten[buurten[columns].eq(-99999999).any(axis=1)]
buurten = pd.concat([buurten, incomplete_buurten]).drop_duplicates(keep=False)

######

neighbourhood_df_complete = pd.DataFrame()
buurt_count = 0

for index, buurt in buurten.iterrows():
    print('buurt no', buurt_count, 'buurt: ',
          buurt['bu_naam'], 'gemeente: ', buurt['gm_naam'])
    buurt_count += 1
    # print(buurt)
    gebouw_oppervlaktes = buildings[buildings['bu_code'] == buurt['bu_code']]
    woning_oppervlaktes = gebouw_oppervlaktes[gebouw_oppervlaktes['gebruiksdoelverblijfsobject'] == 'woonfunctie'].sort_values(
        by='oppervlakteverblijfsobject').reset_index(drop=True)
    niet_woningen = gebouw_oppervlaktes[gebouw_oppervlaktes['gebruiksdoelverblijfsobject']
                                        != 'woonfunctie'].reset_index(drop=True)

    # retrieve square meter per household type per person
    avg_m2_one_p_hh = average_m2_per_one_person_hh_per_municipality[average_m2_per_one_person_hh_per_municipality[
        '\xa0'] == buurt['gm_naam']].iloc[0]['Opp. per persoon, Eenpersoonshuishoudens ( m2)']
    avg_m2_one_p_hh = float(avg_m2_one_p_hh.replace(',', '.'))

    avg_m2_pp_with_kids = average_m2_per_person_in_couples_with_kids_per_municipality[
        average_m2_per_person_in_couples_with_kids_per_municipality['\xa0'] == buurt['gm_naam']].iloc[0]['Opp. per persoon, Paren met kinderen ( m2)']
    avg_m2_pp_with_kids = float(avg_m2_pp_with_kids.replace(',', '.'))

    average_m2_ppwithout_kids = average_m2_per_person_in_couples_without_kids_per_municipality[
        average_m2_per_person_in_couples_without_kids_per_municipality['\xa0'] == buurt['gm_naam']].iloc[0]['Opp. per persoon,  Paren zonder kinderen ( m2)']
    average_m2_ppwithout_kids = float(
        average_m2_ppwithout_kids.replace(',', '.'))

    average_m2_pp = average_m2_per_person_per_municipality[average_m2_per_person_per_municipality['\xa0']
                                                           == buurt['gm_naam']].iloc[0]['Opp. per persoon, Alle huishoudens ( m2)']
    average_m2_pp = float(average_m2_pp.replace(',', '.'))

    # get age groups for the municipality we are in with the current neighbourhood
    age_groups = getAgeGroupsPerMunicipality(
        leeftijden_per_huishouden, buurt['gm_code'])

    # municipality values
    statistics_0_14 = getStatisticsForAgeGroup(age_groups['0-14'])
    statistics_15_24 = getStatisticsForAgeGroup(age_groups['15-24'])
    statistics_25_44 = getStatisticsForAgeGroup(age_groups['25-44'])
    statistics_45_64 = getStatisticsForAgeGroup(age_groups['45-64'])
    statistics_65plus = getStatisticsForAgeGroup(age_groups['65plus'])

    # municipality percentages
    percentages_0_14 = calculateHousholdDistributionPercentages(
        statistics_0_14)
    percentages_15_24 = calculateHousholdDistributionPercentages(
        statistics_15_24)
    percentages_25_44 = calculateHousholdDistributionPercentages(
        statistics_25_44)
    percentages_45_64 = calculateHousholdDistributionPercentages(
        statistics_45_64)
    percentages_65plus = calculateHousholdDistributionPercentages(
        statistics_65plus)

    inwoners = buurt['aant_inw']
    huishoudens = buurt['aantal_hh']

    # huishoudens met 1 persoon
    aantal_1p_huishoudens = round(huishoudens / 100 * buurt['p_eenp_hh'])
    p_aantal_1p_huishoudens = buurt['p_eenp_hh']

    # 2 of meer personen in huishouden
    # niet gehuwde paren zonder kinderen
    # echt-paren zonder kinderen
    # meerdere samenwonende personen
    aantal_huishoudens_zonder_kinderen = round(
        huishoudens / 100 * buurt['p_hh_z_k'])
    p_aantal_huishoudens_zonder_kinderen = buurt['p_hh_z_k']

    # 2 of meer personen in huishouden
    # niet-gehuwde paren met kinderen
    # echtparen met kinderen
    # eenouderhuishoudens
    aantal_huishoudens_met_kinderen = round(
        huishoudens / 100 * buurt['p_hh_m_k'])
    p_aantal_huishoudens_met_kinderen = buurt['p_hh_m_k']

    aantal_0_14 = round(inwoners / 100 * buurt['p_00_14_jr'])
    aantal_15_24 = round(inwoners / 100 * buurt['p_15_24_jr'])
    aantal_25_44 = round(inwoners / 100 * buurt['p_25_44_jr'])
    aantal_45_64 = round(inwoners / 100 * buurt['p_45_64_jr'])
    aantal_65plus = round(inwoners / 100 * buurt['p_65_eo_jr'])

    # project municipality percentages to neighbourhood values
    projected_values_0_14 = householdsDistributionPercentagesToValues(
        aantal_0_14, percentages_0_14)
    projected_values_15_24 = householdsDistributionPercentagesToValues(
        aantal_15_24, percentages_15_24)
    projected_values_25_54 = householdsDistributionPercentagesToValues(
        aantal_25_44, percentages_25_44)
    projected_values_45_64 = householdsDistributionPercentagesToValues(
        aantal_45_64, percentages_45_64)
    projected_values_65plus = householdsDistributionPercentagesToValues(
        aantal_65plus, percentages_65plus)
    projected_values = [projected_values_0_14, projected_values_15_24,
                        projected_values_25_54, projected_values_45_64, projected_values_65plus]

    partners_with_kids = list(
        map(lambda x: x['total households with children'], projected_values))
    partners_without_kids = list(
        map(lambda x: x['total households without children'], projected_values))
    singles = list(map(lambda x: x['singles'], projected_values))
    children = list(map(lambda x: x['children'], projected_values))
    single_parents = list(map(lambda x: x['single parents'], projected_values))

    projected_totals = np.array(
        [children, singles, partners_without_kids, partners_with_kids, single_parents])
    projected_totals = projected_totals.transpose()
    projected_totals_df = pd.DataFrame(projected_totals, columns=[
                                       'children', 'singles', 'partner without children', 'partner with children', 'single parents'])

    # retrieve percentages per householdsize from the municipality the neighbourhood is in
    percentages_per_householdsize = getHousholdSizePercentagesPerMunicipality(
        gemeente_data, buurt['gm_naam'])

    # calculate the amount of single parents for later use
    amount_of_single_parents = getAmountOfSingleParents(
        percentages_per_householdsize, aantal_huishoudens_met_kinderen)

    # calculate difference between projected sum data and actual sum data
    singles_diff = aantal_1p_huishoudens - \
        sum(projected_totals_df['singles'].values)
    single_parents_diff = amount_of_single_parents - \
        sum(projected_totals_df['single parents'].values)
    partner_no_children_diff = aantal_huishoudens_zonder_kinderen * \
        2 - sum(projected_totals_df['partner without children'].values)
    partner_children_diff = aantal_huishoudens_met_kinderen*2 - \
        sum(projected_totals_df['partner with children'].values)

    # calculate multipliers for each household group to scale up or down in values to get the exact values needed for a neighbourhood
    # based on the municipality data
    multipliers = getIncrementMultipliers(
        np.array(projected_totals_df['singles'].values))
    singles = adjust_household_distribution(
        projected_totals_df['singles'], multipliers, singles_diff)

    multipliers = getIncrementMultipliers(
        np.array(projected_totals_df['single parents'].values))
    single_parents = adjust_household_distribution(
        projected_totals_df['single parents'], multipliers, single_parents_diff)

    multipliers = getIncrementMultipliers(
        np.array(projected_totals_df['partner without children'].values))
    partner_no_children = adjust_household_distribution(
        projected_totals_df['partner without children'], multipliers, partner_no_children_diff)

    multipliers = getIncrementMultipliers(
        np.array(projected_totals_df['partner with children'].values))
    partner_with_children = adjust_household_distribution(
        projected_totals_df['partner with children'], multipliers, partner_children_diff)

    # calculate children based on previous children
    all_except_children = sum([sum(singles), sum(single_parents), sum(
        partner_no_children), sum(partner_with_children)])
    children_diff = (inwoners - all_except_children) - \
        sum(projected_totals_df['children'].values)
    # children_multipliers = getChildrenMultipliersPerMunicipality(age_groups)
    children_multipliers = getChildrenMultipliersPerMunicipality(
        np.array(projected_totals_df['children'].iloc[1:].values))
    children = adjust_children(
        projected_totals_df['children'], children_diff, children_multipliers)

    # assign adjusted projected values per household group to new dataframe
    adjusted_projected_totals_df = projected_totals_df.copy()
    adjusted_projected_totals_df['children'] = children
    adjusted_projected_totals_df['singles'] = singles
    adjusted_projected_totals_df['partner without children'] = partner_no_children
    adjusted_projected_totals_df['partner with children'] = partner_with_children
    adjusted_projected_totals_df['single parents'] = single_parents

    # calculate the amount of households per size based on the percentages from cbs in this municipality
    # also adjust the amount of households based on the amount of residential buildings in the BAG data
    hh_per_size = getAmountOfHouseholdsPerSize(
        percentages_per_householdsize, aantal_1p_huishoudens, huishoudens)

    # create lists and dicts for each age group and each household type
    # these values are used while forming families and filling the resulting dataset
    one_person_hh_age_groups = adjusted_projected_totals_df['singles'].iloc[0] * ['0-14'] + adjusted_projected_totals_df['singles'].iloc[1] * [
        '15-24'] + adjusted_projected_totals_df['singles'].iloc[2] * ['25-44'] + adjusted_projected_totals_df['singles'].iloc[3] * ['45-64'] + adjusted_projected_totals_df['singles'].iloc[4] * ['65plus']

    household_without_children = round(adjusted_projected_totals_df['partner without children'].iloc[0] / 2) * ['0-14'] + round(adjusted_projected_totals_df['partner without children'].iloc[1] / 2) * ['15-24'] + round(
        adjusted_projected_totals_df['partner without children'].iloc[2] / 2) * ['25-44'] + round(adjusted_projected_totals_df['partner without children'].iloc[3] / 2) * ['45-64'] + round(adjusted_projected_totals_df['partner without children'].iloc[4] / 2) * ['65plus']

    single_parents_15_24 = [{'parent': '15-24', 'single parent': True, 'children': []}
                            for k in range(adjusted_projected_totals_df['single parents'].iloc[1])]
    single_parents_25_44 = [{'parent': '25-44', 'single parent': True, 'children': []}
                            for k in range(adjusted_projected_totals_df['single parents'].iloc[2])]
    single_parents_45_64 = [{'parent': '45-64', 'single parent': True, 'children': []}
                            for k in range(adjusted_projected_totals_df['single parents'].iloc[3])]
    single_parents_65plus = [{'parent': '65plus', 'single parent': True, 'children': [
    ]} for k in range(adjusted_projected_totals_df['single parents'].iloc[4])]

    household_with_children_15_24 = [{'parent': '15-24', 'single parent': False, 'children': []}
                                     for k in range(round(adjusted_projected_totals_df['partner with children'].iloc[1] / 2))]
    household_with_children_25_44 = [{'parent': '25-44', 'single parent': False, 'children': []}
                                     for k in range(round(adjusted_projected_totals_df['partner with children'].iloc[2] / 2))]
    household_with_children_45_64 = [{'parent': '45-64', 'single parent': False, 'children': []}
                                     for k in range(round(adjusted_projected_totals_df['partner with children'].iloc[3] / 2))]
    household_with_children_65plus = [{'parent': '65plus', 'single parent': False, 'children': [
    ]} for k in range(round(adjusted_projected_totals_df['partner with children'].iloc[4] / 2))]

    children_0_14 = adjusted_projected_totals_df['children'].iloc[0] * ['0-14']
    children_15_24 = adjusted_projected_totals_df['children'].iloc[1] * [
        '15-24']
    children_25_44 = adjusted_projected_totals_df['children'].iloc[2] * [
        '25-44']
    children_45_64 = adjusted_projected_totals_df['children'].iloc[3] * [
        '45-64']
    children_65_plus = adjusted_projected_totals_df['children'].iloc[4] * [
        '65plus']

    amount_of_65_plus_hh = len(household_with_children_65plus)
    amount_of_45_64_hh = len(household_with_children_45_64)
    amount_of_25_44_hh = len(household_with_children_25_44)
    amount_of_15_24_hh = len(household_with_children_15_24)

    # assign all households and children to a single variable so we can iterate through them and assign children to households
    all_children = children_0_14 + children_15_24 + \
        children_25_44 + children_45_64 + children_65_plus
    all_hh_with_children = household_with_children_15_24+household_with_children_25_44+household_with_children_45_64 + \
        household_with_children_65plus+single_parents_65plus + \
        single_parents_45_64 + single_parents_25_44 + single_parents_15_24

    # assign children to a queue for to process the children in an easier way
    # also have the possibility to shuffle the children and households for a more random distribution
    # random.shuffle(all_hh_with_children)
    # random.shuffle(all_children)
    children_queue = queue.Queue()
    children_queue.queue = queue.deque(all_children)

    # keep counts of amount of children assigned for checking in the assignment process
    amount_of_1children_hh = 0
    amount_of_2children_hh = 0
    amount_of_3children_hh = 0

    count = 0
    retry_count = 0
    previous_queue_size = children_queue.qsize()
    if len(all_hh_with_children) != 0:
        limit = round(children_queue.qsize() / len(all_hh_with_children))

        while not children_queue.empty():

            # if queue size is the same after x loops quit
            count += 1

            if count == limit:
                count = 0
                if previous_queue_size == children_queue.qsize():
                    retry_count += 1
                previous_queue_size = children_queue.qsize()
                if retry_count == 3:
                    break

            # for each household get a child from queue and check if child can fit in household
            for hh in all_hh_with_children:

                if not children_queue.empty():
                    children_in_hh = len(hh['children'])

                    # if limit of households has been met, dump children
                    if sum([amount_of_1children_hh, amount_of_2children_hh, amount_of_3children_hh]) == len(all_hh_with_children) and amount_of_3children_hh == hh_per_size['five person'] and amount_of_2children_hh == hh_per_size['four person']:
                        children_queue.get()
                    else:
                        child = children_queue.get()

                        # check if child can fit in the family, else put child back in queue and proceed to next household
                        if children_in_hh == 0 or (children_in_hh == 1 and amount_of_2children_hh < hh_per_size['four person']) or (children_in_hh == 2 and amount_of_3children_hh < hh_per_size['five person']):
                            if hh['parent'] == '65plus':
                                hh['children'].append(child)
                            elif hh['parent'] == '45-64' and child != '65plus':
                                hh['children'].append(child)
                            elif hh['parent'] == '25-44' and child != '65plus' and child != '45-64':
                                hh['children'].append(child)
                            elif hh['parent'] == '15-24' and child != '65plus' and child != '45-64' and child != '25-44' and child != '15-24':
                                hh['children'].append(child)
                        else:
                            children_queue.put(child)

                        # keep track of the household sizes
                        if children_in_hh != len(hh['children']):
                            if len(hh['children']) == 1:
                                amount_of_1children_hh += 1
                            elif len(hh['children']) == 2:
                                amount_of_1children_hh -= 1
                                amount_of_2children_hh += 1
                            elif len(hh['children']) == 3:
                                amount_of_2children_hh -= 1
                                amount_of_3children_hh += 1

    # convert all households to same format
    children_households = []
    for hh in all_hh_with_children:
        parents_age_group = {'parents_age_group': hh['parent']}
        single_parent = hh['single parent']
        parents_count = {'parent_count': 1 if single_parent else 2}
        children = children_count_per_age_group(hh['children'])
        children_count = {'amount_of_children': len(hh['children'])}
        hh_size = {
            'hh_size': children_count['amount_of_children'] + parents_count['parent_count']}
        category = {
            'household_type': 'couples with children' if not single_parent else 'single parent'}

        children_households.append(
            {**parents_age_group, **parents_count, **children, **children_count, **hh_size, **category})

    one_p_households = []

    for hh in one_person_hh_age_groups:
        parents_age_group = {'parents_age_group': hh}
        parents_count = {'parent_count': 1}
        children = {'0-14': 0, '15-24': 0, '25-44': 0, '45-64': 0, '65plus': 0}
        children_count = {'amount_of_children': 0}
        hh_size = {'hh_size': 1}
        category = {'household_type': 'single'}

        one_p_households.append({**parents_age_group, **parents_count,
                                **children, **children_count, **hh_size, **category})

    no_children_households = []

    for hh in household_without_children:
        parents_age_group = {'parents_age_group': hh}
        parents_count = {'parent_count': 2}
        children = {'0-14': 0, '15-24': 0, '25-44': 0, '45-64': 0, '65plus': 0}
        children_count = {'amount_of_children': 0}
        hh_size = {'hh_size': 2}
        category = {'household_type': 'couples without children'}

        no_children_households.append(
            {**parents_age_group, **parents_count, **children, **children_count, **hh_size, **category})

    # add households to a dataframe
    df_households = pd.DataFrame(
        children_households + one_p_households + no_children_households)

    # print('hh with children', all_hh_with_children)

    singles_df = df_households[df_households['household_type']
                               == 'single'].copy().reset_index(drop=True)
    singles_df['house_size'] = singles_df['hh_size'] * avg_m2_one_p_hh

    single_parents_df = df_households[df_households['household_type']
                                      == 'single parent'].copy().reset_index(drop=True)
    single_parents_df['house_size'] = single_parents_df['hh_size'] * \
        avg_m2_pp_with_kids

    couples_no_children_df = df_households[df_households['household_type']
                                           == 'couples without children'].copy().reset_index(drop=True)
    couples_no_children_df['house_size'] = couples_no_children_df['hh_size'] * \
        average_m2_ppwithout_kids

    couples_with_children_df = df_households[df_households['household_type']
                                             == 'couples with children'].copy().reset_index(drop=True)
    couples_with_children_df['house_size'] = couples_with_children_df['hh_size'] * \
        avg_m2_pp_with_kids

    # make up difference in households
    diff = abs(len(df_households) - len(woning_oppervlaktes))
    # print('diff', diff)
    # print('df_households', len(df_households))
    # print('woning_oppervlaktes', len(woning_oppervlaktes))

    # remove is used to determine if x amount of rows need to be deleted from the dataframe to meet the amount of required households
    remove = len(df_households) > len(woning_oppervlaktes)

    # one person households
    count_1p = round(diff * (p_aantal_1p_huishoudens / 100))

    if len(singles_df) > 0:
        if count_1p > len(singles_df):
            # print('more 1p then available')
            n = int((count_1p - (count_1p % len(singles_df))) / len(singles_df))
            singles_df = pd.concat(
                [singles_df]*(n), ignore_index=True).reset_index(drop=True)
            # singles_df.loc[singles_df.index.repeat(n)].reset_index(drop=True)
            count_1p = (count_1p % len(singles_df))

        indices = np.random.choice(singles_df.index, count_1p, replace=False)
        singles_df = singles_df.drop(
            indices) if remove else singles_df.append(singles_df.iloc[indices])

    # combine single parents and couples with children into a general dataframe for adding/removing households with children
    hh_with_children_df = pd.concat([single_parents_df.copy(
    ), couples_with_children_df.copy()]).reset_index(drop=True)

    # family with children households
    count_children = round(diff * (p_aantal_huishoudens_met_kinderen / 100))

    # print(count_children)

    if len(hh_with_children_df) > 0:
        if count_children > len(hh_with_children_df):
            # print('more hh with children then available')
            n = int((count_children - (count_children %
                    len(hh_with_children_df))) / len(hh_with_children_df))
            hh_with_children_df = pd.concat(
                [hh_with_children_df]*(n), ignore_index=True).reset_index(drop=True)
            count_children = (count_children % len(hh_with_children_df))

        indices = np.random.choice(
            hh_with_children_df.index, count_children, replace=False)
        hh_with_children_df = hh_with_children_df.drop(
            indices) if remove else hh_with_children_df.append(hh_with_children_df.iloc[indices])

    # family without children households
    count_no_children = round(
        diff * (p_aantal_huishoudens_zonder_kinderen / 100))

    if len(couples_no_children_df) > 0:
        if count_no_children > len(couples_no_children_df):
            # print('more hh with children then available')
            n = int((count_no_children - (count_no_children %
                    len(couples_no_children_df))) / len(couples_no_children_df))
            couples_no_children_df = pd.concat(
                [couples_no_children_df]*(n), ignore_index=True).reset_index(drop=True)
            count_no_children = (count_no_children %
                                 len(couples_no_children_df))

        indices = np.random.choice(
            couples_no_children_df.index, count_no_children, replace=False)
        couples_no_children_df = couples_no_children_df.drop(
            indices) if remove else couples_no_children_df.append(couples_no_children_df.iloc[indices])

    # assign the adjusted rows to a dataframe
    df_households = pd.concat(
        [singles_df, couples_no_children_df, hh_with_children_df]).reset_index(drop=True)

    # fix the total amount of households if rounding went wrong ( total of rounded values could be off by a couple households due to rounding values up or down)
    if len(df_households) != len(woning_oppervlaktes):
        len_w = len(woning_oppervlaktes)
        len_hh = len(df_households)
        n = abs(len_hh - len_w)
        remove = len(df_households) > len(woning_oppervlaktes)

        indices = np.random.choice(df_households.index, n, replace=False)

        # print('chosen indices', len(indices))
        # print('remove', len(df_households) > len(woning_oppervlaktes))

        df_households = df_households.drop(
            indices) if remove == True else df_households.append(df_households.iloc[indices])

    df_households = df_households.reset_index(drop=True)

    # print('check same count', len(df_households) == len(woning_oppervlaktes), len(df_households) - len(woning_oppervlaktes))

    # sort the households based on house size
    df_households = df_households.sort_values(
        by='house_size').reset_index(drop=True)

    df_non_households = assign_max_people_in_building(niet_woningen)

    woning_oppervlaktes = woning_oppervlaktes.reset_index(drop=True)

    # merge residential buildings with the households to finalize population assignment
    merged_final_df = df_households.merge(
        woning_oppervlaktes, left_index=True, right_index=True)
    merged_final_df = merged_final_df.append(df_non_households)
    # print(merged_final_df)
    # print(len(gebouw_oppervlaktes))

    # print('same amount of buildings', len(df_households) + len(df_non_households) == len(buildings[buildings['bu_code'] == buurt['bu_code']]))
    del merged_final_df['house_size']
    neighbourhood_df_complete = neighbourhood_df_complete.append(
        merged_final_df)


incomplete_neighbourhoods = {'buurten': {}}

buurt_count = 0

for index, buurt in incomplete_buurten.iterrows():
    buurt_count += 1
    gebouw_oppervlaktes = buildings[buildings['bu_code'] == buurt['bu_code']]
    woning_oppervlaktes = gebouw_oppervlaktes[gebouw_oppervlaktes['gebruiksdoelverblijfsobject'] == 'woonfunctie'].sort_values(
        by='oppervlakteverblijfsobject').reset_index(drop=True)
    niet_woningen = pd.concat(
        [gebouw_oppervlaktes, woning_oppervlaktes]).drop_duplicates(keep=False)

    print('buurt no', buurt_count, 'buurt: ',
          buurt['bu_naam'], 'gemeente: ', buurt['gm_naam'])
    # print('len woningen', woning_oppervlaktes)

    if buurt['aantal_hh'] != 0 and buurt['aantal_hh'] != -99999999:
        if buurt['p_eenp_hh'] != -99999999 and buurt['p_hh_z_k'] != -99999999 and buurt['p_hh_m_k'] != -99999999:
            age_groups = getAgeGroupsPerMunicipality(
                leeftijden_per_huishouden, buurt['gm_code'])

            # retrieve square meter per household type per person
            avg_m2_one_p_hh = average_m2_per_one_person_hh_per_municipality[average_m2_per_one_person_hh_per_municipality[
                '\xa0'] == buurt['gm_naam']].iloc[0]['Opp. per persoon, Eenpersoonshuishoudens ( m2)']
            avg_m2_one_p_hh = float(avg_m2_one_p_hh.replace(',', '.'))

            avg_m2_pp_with_kids = average_m2_per_person_in_couples_with_kids_per_municipality[
                average_m2_per_person_in_couples_with_kids_per_municipality['\xa0'] == buurt['gm_naam']].iloc[0]['Opp. per persoon, Paren met kinderen ( m2)']
            avg_m2_pp_with_kids = float(avg_m2_pp_with_kids.replace(',', '.'))

            average_m2_ppwithout_kids = average_m2_per_person_in_couples_without_kids_per_municipality[
                average_m2_per_person_in_couples_without_kids_per_municipality['\xa0'] == buurt['gm_naam']].iloc[0]['Opp. per persoon,  Paren zonder kinderen ( m2)']
            average_m2_ppwithout_kids = float(
                average_m2_ppwithout_kids.replace(',', '.'))

            average_m2_pp = average_m2_per_person_per_municipality[average_m2_per_person_per_municipality[
                '\xa0'] == buurt['gm_naam']].iloc[0]['Opp. per persoon, Alle huishoudens ( m2)']
            average_m2_pp = float(average_m2_pp.replace(',', '.'))

            amount_1p = round(buurt['aantal_hh'] / 100 * buurt['p_eenp_hh'])
            amount_hh_z_k = round(buurt['aantal_hh'] / 100 * buurt['p_hh_z_k'])
            amount_hh_m_k = round(buurt['aantal_hh'] / 100 * buurt['p_hh_m_k'])
            amount_children = buurt['aant_inw'] - \
                amount_1p - (amount_hh_z_k*2) - (amount_hh_m_k*2)

            if amount_hh_m_k > 0:
                if amount_children > amount_hh_m_k:
                    children_per_hh = [int(
                        (amount_children - (amount_children % amount_hh_m_k)) / amount_hh_m_k)] * amount_hh_m_k
                    children_left_over = amount_children % amount_hh_m_k
                    for i in range(0, children_left_over):
                        children_per_hh[i] += 1

                else:
                    children_per_hh = []
                    for i in range(0, amount_hh_m_k):
                        children_per_hh.append(1)

            children_households = []

            for hh in range(0, amount_hh_m_k):
                amount_of_children = children_per_hh[hh]
                # print(amount_of_children)
                parents_age_group = {'parents_age_group': 'unknown'}
                single_parent = False
                parents_count = {'parent_count': 2}
                children = {'0-14': amount_of_children, '15-24': 0,
                            '25-44': 0, '45-64': 0, '65plus': 0}
                children_count = {'amount_of_children': amount_of_children}
                hh_size = {'hh_size': (amount_of_children + 2)}
                category = {'household_type': 'couples with children'}

                children_households.append(
                    {**parents_age_group, **parents_count, **children, **children_count, **hh_size, **category})

            one_p_households = []

            for hh in range(0, amount_1p):
                parents_age_group = {'parents_age_group': 'unknown'}
                parents_count = {'parent_count': 1}
                children = {'0-14': 0, '15-24': 0,
                            '25-44': 0, '45-64': 0, '65plus': 0}
                children_count = {'amount_of_children': 0}
                hh_size = {'hh_size': 1}
                category = {'household_type': 'single'}

                one_p_households.append(
                    {**parents_age_group, **parents_count, **children, **children_count, **hh_size, **category})

            no_children_households = []

            for hh in range(0, amount_hh_z_k):
                parents_age_group = {'parents_age_group': 'unknown'}
                parents_count = {'parent_count': 2}
                children = {'0-14': 0, '15-24': 0,
                            '25-44': 0, '45-64': 0, '65plus': 0}
                children_count = {'amount_of_children': 0}
                hh_size = {'hh_size': 2}
                category = {'household_type': 'couples without children'}

                no_children_households.append(
                    {**parents_age_group, **parents_count, **children, **children_count, **hh_size, **category})

            # add households to a dataframe
            df_households = pd.DataFrame(
                children_households + one_p_households + no_children_households).reset_index(drop=True)

            if len(df_households) != len(woning_oppervlaktes):
                len_w = len(woning_oppervlaktes)
                len_hh = len(df_households)
                n = abs(len_hh - len_w)

                if len_w > len_hh:
                    # print('more hh with children then available')
                    n = int((len_w - (len_w % len_hh)) / len_hh)
                    df_households = pd.concat(
                        [df_households]*(n), ignore_index=True).reset_index(drop=True)
                    n = (len_w % len_hh)

                indices = np.random.choice(
                    df_households.index, n, replace=False)

                df_households = df_households.drop(indices) if len(df_households) > len(
                    woning_oppervlaktes) else df_households.append(df_households.iloc[indices])

            df_households = df_households.reset_index(drop=True)

            singles_df = df_households[df_households['household_type'] == 'single'].copy(
            ).reset_index(drop=True)
            singles_df['house_size'] = singles_df['hh_size'] * avg_m2_one_p_hh

            couples_no_children_df = df_households[df_households['household_type']
                                                   == 'couples without children'].copy().reset_index(drop=True)
            couples_no_children_df['house_size'] = couples_no_children_df['hh_size'] * \
                average_m2_ppwithout_kids

            couples_with_children_df = df_households[df_households['household_type'] == 'couples with children'].copy(
            ).reset_index(drop=True)
            couples_with_children_df['house_size'] = couples_with_children_df['hh_size'] * \
                avg_m2_pp_with_kids

            df_households = pd.concat(
                [singles_df, couples_no_children_df, couples_with_children_df])

            # sort the households based on house size
            df_households = df_households.sort_values(
                by='house_size').reset_index(drop=True)

            # print('check same count 1', len(df_households) == len(woning_oppervlaktes), len(df_households) - len(woning_oppervlaktes))

            # merge residential buildings with the households to finalize population assignment
            merged_final_df = df_households.merge(
                woning_oppervlaktes, left_index=True, right_index=True)
            # print(merged_final_df.to_string())

        else:
            amount_persons_per_hh = round(
                buurt['aant_inw'] / buurt['aantal_hh'])

            households = [amount_persons_per_hh] * buurt['aantal_hh']

            # print('households', households)

            if amount_persons_per_hh == 1:
                amount_1p = households
            else:
                amount_1p = []

            if amount_persons_per_hh == 2:
                amount_hh_z_k = households
            else:
                amount_hh_z_k = []

            if amount_persons_per_hh > 2:
                amount_hh_m_k = households
            else:
                amount_hh_m_k = []

            one_p_households = []

            for hh in amount_1p:
                parents_age_group = {'parents_age_group': 'unknown'}
                parents_count = {'parent_count': 1}
                children = {'0-14': 0, '15-24': 0,
                            '25-44': 0, '45-64': 0, '65plus': 0}
                children_count = {'amount_of_children': 0}
                hh_size = {'hh_size': 1}
                category = {'household_type': 'single'}

                one_p_households.append(
                    {**parents_age_group, **parents_count, **children, **children_count, **hh_size, **category})

            no_children_households = []

            for hh in amount_hh_z_k:
                parents_age_group = {'parents_age_group': 'unknown'}
                parents_count = {'parent_count': 2}
                children = {'0-14': 0, '15-24': 0,
                            '25-44': 0, '45-64': 0, '65plus': 0}
                children_count = {'amount_of_children': 0}
                hh_size = {'hh_size': 2}
                category = {'household_type': 'couples without children'}

                no_children_households.append(
                    {**parents_age_group, **parents_count, **children, **children_count, **hh_size, **category})

            children_households = []

            for hh in amount_hh_m_k:
                # print(hh)
                parents_age_group = {'parents_age_group': 'unknown'}
                single_parent = False
                parents_count = {'parent_count': 2}
                children = {'0-14': (hh - 2), '15-24': 0,
                            '25-44': 0, '45-64': 0, '65plus': 0}
                children_count = {'amount_of_children': (hh-2)}
                hh_size = {'hh_size': hh}
                category = {'household_type': 'couples with children'}

                children_households.append(
                    {**parents_age_group, **parents_count, **children, **children_count, **hh_size, **category})

            # add households to a dataframe
            df_households = pd.DataFrame(
                children_households + one_p_households + no_children_households).reset_index(drop=True)

            # fix the total amount of households if rounding went wrong ( total of rounded values could be off by a couple households due to rounding values up or down)
            # print(len(df_households))
            # print(len(woning_oppervlaktes))

            if len(df_households) != len(woning_oppervlaktes):
                len_w = len(woning_oppervlaktes)
                len_hh = len(df_households)
                n = abs(len_hh - len_w)
                remove = len(df_households) > len(woning_oppervlaktes)

                if len_w > len(df_households):
                    n = int((len_w - (len_w % len(df_households))) /
                            len(df_households))

                    df_households = pd.concat(
                        [df_households]*(n), ignore_index=True).reset_index(drop=True)

                    # singles_df.loc[singles_df.index.repeat(n)].reset_index(drop=True)
                    n = (len_w % len_hh)

                indices = np.random.choice(
                    df_households.index, n, replace=False)

                # print('chosen indices', len(indices))
                # print('remove', len(df_households) > len(woning_oppervlaktes))

                df_households = df_households.drop(
                    indices) if remove == True else df_households.append(df_households.iloc[indices])

            df_households = df_households.reset_index(drop=True)

            # print('check same count 2', len(df_households) == len(woning_oppervlaktes), len(df_households) - len(woning_oppervlaktes))

            # merge residential buildings with the households to finalize population assignment
            merged_final_df = df_households.merge(
                woning_oppervlaktes, left_index=True, right_index=True)
            # print(merged_final_df.to_string())

    else:

        empty_households = []

        for index, woning in woning_oppervlaktes.iterrows():
            parents_age_group = {'parents_age_group': 'unknown'}
            parents_count = {'parent_count': 0}
            children = {'0-14': 0, '15-24': 0,
                        '25-44': 0, '45-64': 0, '65plus': 0}
            children_count = {'amount_of_children': 0}
            hh_size = {'hh_size': 0}
            category = {'household_type': 'empty'}

            empty_households.append(
                {**parents_age_group, **parents_count, **children, **children_count, **hh_size, **category})

        df_households = pd.DataFrame(empty_households)

        merged_final_df = df_households.merge(
            woning_oppervlaktes, left_index=True, right_index=True)

    df_non_households = assign_max_people_in_building(niet_woningen)

    merged_final_df = merged_final_df.append(df_non_households)

    neighbourhood_df_complete = neighbourhood_df_complete.append(
        merged_final_df)

del neighbourhood_df_complete['house_size']

neighbourhood_df_complete = neighbourhood_df_complete.reset_index(drop=True)
neighbourhood_df_complete = neighbourhood_df_complete.rename(
    columns={'0-14': 'ag_00_14', '15-24': 'ag_15_24', '25-44': 'ag_25_44', '45-64': 'ag_45_64', '65plus': 'ag_65_plus'})


neighbourhood_df_complete['parents_age_group'] = neighbourhood_df_complete['parents_age_group'].astype(
    str)
neighbourhood_df_complete['household_type'] = neighbourhood_df_complete['household_type'].astype(
    str)
neighbourhood_df_complete['bu_code'] = neighbourhood_df_complete['bu_code'].astype(
    str)
neighbourhood_df_complete['identificatie'] = neighbourhood_df_complete['identificatie'].astype(
    str)
neighbourhood_df_complete['gebruiksdoelverblijfsobject'] = neighbourhood_df_complete['gebruiksdoelverblijfsobject'].astype(
    str)

neighbourhood_df_complete['ag_00_14'] = neighbourhood_df_complete['ag_00_14'].astype(
    int)
neighbourhood_df_complete['ag_15_24'] = neighbourhood_df_complete['ag_15_24'].astype(
    int)
neighbourhood_df_complete['ag_25_44'] = neighbourhood_df_complete['ag_25_44'].astype(
    int)
neighbourhood_df_complete['ag_45_64'] = neighbourhood_df_complete['ag_45_64'].astype(
    int)
neighbourhood_df_complete['ag_65_plus'] = neighbourhood_df_complete['ag_65_plus'].astype(
    int)
neighbourhood_df_complete['amount_of_children'] = neighbourhood_df_complete['amount_of_children'].astype(
    int)
neighbourhood_df_complete['hh_size'] = neighbourhood_df_complete['hh_size'].astype(
    int)
neighbourhood_df_complete['parent_count'] = neighbourhood_df_complete['parent_count'].astype(
    int)

neighbourhood_df_complete.to_csv('population.csv', index_label='id')
