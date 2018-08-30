package main

import (
	"log"
	"strconv"
	"strings"
	"time"
)

func ProductFromCsvRecord(record []string, n int) (p *Product) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("record on line %v: %v\n", n, r)
			p = nil
		}
	}()

	p = &Product{
		Code:    record[0],
		Url:     record[1],
		Creator: record[2],
		// Created_t:                 record[3],
		Created_datetime: parseDatetime(record[4]),
		// Last_modified_t:           record[5],
		Last_modified_datetime:    parseDatetime(record[6]),
		Product_name:              record[7],
		Generic_name:              record[8],
		Quantity:                  record[9],
		Packaging:                 splitFields(record[10]),
		Packaging_tags:            splitFields(record[11]),
		Brands:                    splitFields(record[12]),
		Brands_tags:               splitFields(record[13]),
		Categories:                splitFields(record[14]),
		Categories_tags:           splitFields(record[15]),
		Categories_en:             splitFields(record[16]),
		Origins:                   splitFields(record[17]),
		Origins_tags:              splitFields(record[18]),
		Manufacturing_places:      splitFields(record[19]),
		Manufacturing_places_tags: splitFields(record[20]),
		Labels:                   splitFields(record[21]),
		Labels_tags:              splitFields(record[22]),
		Labels_en:                splitFields(record[23]),
		Emb_codes:                splitFields(record[24]),
		Emb_codes_tags:           splitFields(record[25]),
		First_packaging_code_geo: record[26],
		Cities:                                     splitFields(record[27]),
		Cities_tags:                                splitFields(record[28]),
		Purchase_places:                            splitFields(record[29]),
		Stores:                                     splitFields(record[30]),
		Countries:                                  splitFields(record[31]),
		Countries_tags:                             splitFields(record[32]),
		Countries_en:                               splitFields(record[33]),
		Ingredients_text:                           record[34],
		Allergens:                                  record[35],
		Allergens_en:                               record[36],
		Traces:                                     splitFields(record[37]),
		Traces_tags:                                splitFields(record[38]),
		Traces_en:                                  splitFields(record[39]),
		Serving_size:                               record[40],
		Serving_quantity:                           parseFloat32(record[41]),
		No_nutriments:                              record[42],
		Additives_n:                                parseInt(record[43]),
		Additives:                                  splitFields(record[44]),
		Additives_tags:                             splitFields(record[45]),
		Additives_en:                               splitFields(record[46]),
		Ingredients_from_palm_oil_n:                parseInt(record[47]),
		Ingredients_from_palm_oil:                  record[48],
		Ingredients_from_palm_oil_tags:             splitFields(record[49]),
		Ingredients_that_may_be_from_palm_oil_n:    parseInt(record[50]),
		Ingredients_that_may_be_from_palm_oil:      record[51],
		Ingredients_that_may_be_from_palm_oil_tags: splitFields(record[52]),
		Nutrition_grade_uk:                         record[53],
		Nutrition_grade_fr:                         record[54],
		Pnns_groups_1:                              record[55],
		Pnns_groups_2:                              record[56],
		States:                                     splitFields(record[57]),
		States_tags:                                splitFields(record[58]),
		States_en:                                  splitFields(record[59]),
		Main_category:                              splitFields(record[60]),
		Main_category_en:                           splitFields(record[61]),
		Image_url:                                  record[62],
		Image_small_url:                            record[63],
		Image_ingredients_url:                      record[64],
		Image_ingredients_small_url:                record[65],
		Image_nutrition_url:                        record[66],
		Image_nutrition_small_url:                  record[67],
		Nutrition: Nutrition{
			Energy_100g:                          parseFloat32(record[68]),
			Energy_from_fat_100g:                 parseFloat32(record[69]),
			Fat_100g:                             parseFloat32(record[70]),
			Saturated_fat_100g:                   parseFloat32(record[71]),
			Butyric_acid_100g:                    parseFloat32(record[72]),
			Caproic_acid_100g:                    parseFloat32(record[73]),
			Caprylic_acid_100g:                   parseFloat32(record[74]),
			Capric_acid_100g:                     parseFloat32(record[75]),
			Lauric_acid_100g:                     parseFloat32(record[76]),
			Myristic_acid_100g:                   parseFloat32(record[77]),
			Palmitic_acid_100g:                   parseFloat32(record[78]),
			Stearic_acid_100g:                    parseFloat32(record[79]),
			Arachidic_acid_100g:                  parseFloat32(record[80]),
			Behenic_acid_100g:                    parseFloat32(record[81]),
			Lignoceric_acid_100g:                 parseFloat32(record[82]),
			Cerotic_acid_100g:                    parseFloat32(record[83]),
			Montanic_acid_100g:                   parseFloat32(record[84]),
			Melissic_acid_100g:                   parseFloat32(record[85]),
			Monounsaturated_fat_100g:             parseFloat32(record[86]),
			Polyunsaturated_fat_100g:             parseFloat32(record[87]),
			Omega_3_fat_100g:                     parseFloat32(record[88]),
			Alpha_linolenic_acid_100g:            parseFloat32(record[89]),
			Eicosapentaenoic_acid_100g:           parseFloat32(record[90]),
			Docosahexaenoic_acid_100g:            parseFloat32(record[91]),
			Omega_6_fat_100g:                     parseFloat32(record[92]),
			Linoleic_acid_100g:                   parseFloat32(record[93]),
			Arachidonic_acid_100g:                parseFloat32(record[94]),
			Gamma_linolenic_acid_100g:            parseFloat32(record[95]),
			Dihomo_gamma_linolenic_acid_100g:     parseFloat32(record[96]),
			Omega_9_fat_100g:                     parseFloat32(record[97]),
			Oleic_acid_100g:                      parseFloat32(record[98]),
			Elaidic_acid_100g:                    parseFloat32(record[99]),
			Gondoic_acid_100g:                    parseFloat32(record[100]),
			Mead_acid_100g:                       parseFloat32(record[101]),
			Erucic_acid_100g:                     parseFloat32(record[102]),
			Nervonic_acid_100g:                   parseFloat32(record[103]),
			Trans_fat_100g:                       parseFloat32(record[104]),
			Cholesterol_100g:                     parseFloat32(record[105]),
			Carbohydrates_100g:                   parseFloat32(record[106]),
			Sugars_100g:                          parseFloat32(record[107]),
			Sucrose_100g:                         parseFloat32(record[108]),
			Glucose_100g:                         parseFloat32(record[109]),
			Fructose_100g:                        parseFloat32(record[110]),
			Lactose_100g:                         parseFloat32(record[111]),
			Maltose_100g:                         parseFloat32(record[112]),
			Maltodextrins_100g:                   parseFloat32(record[113]),
			Starch_100g:                          parseFloat32(record[114]),
			Polyols_100g:                         parseFloat32(record[115]),
			Fiber_100g:                           parseFloat32(record[116]),
			Proteins_100g:                        parseFloat32(record[117]),
			Casein_100g:                          parseFloat32(record[118]),
			Serum_proteins_100g:                  parseFloat32(record[119]),
			Nucleotides_100g:                     parseFloat32(record[120]),
			Salt_100g:                            parseFloat32(record[121]),
			Sodium_100g:                          parseFloat32(record[122]),
			Alcohol_100g:                         parseFloat32(record[123]),
			Vitamin_a_100g:                       parseFloat32(record[124]),
			Beta_carotene_100g:                   parseFloat32(record[125]),
			Vitamin_d_100g:                       parseFloat32(record[126]),
			Vitamin_e_100g:                       parseFloat32(record[127]),
			Vitamin_k_100g:                       parseFloat32(record[128]),
			Vitamin_c_100g:                       parseFloat32(record[129]),
			Vitamin_b1_100g:                      parseFloat32(record[130]),
			Vitamin_b2_100g:                      parseFloat32(record[131]),
			Vitamin_pp_100g:                      parseFloat32(record[132]),
			Vitamin_b6_100g:                      parseFloat32(record[133]),
			Vitamin_b9_100g:                      parseFloat32(record[134]),
			Folates_100g:                         parseFloat32(record[135]),
			Vitamin_b12_100g:                     parseFloat32(record[136]),
			Biotin_100g:                          parseFloat32(record[137]),
			Pantothenic_acid_100g:                parseFloat32(record[138]),
			Silica_100g:                          parseFloat32(record[139]),
			Bicarbonate_100g:                     parseFloat32(record[140]),
			Potassium_100g:                       parseFloat32(record[141]),
			Chloride_100g:                        parseFloat32(record[142]),
			Calcium_100g:                         parseFloat32(record[143]),
			Phosphorus_100g:                      parseFloat32(record[144]),
			Iron_100g:                            parseFloat32(record[145]),
			Magnesium_100g:                       parseFloat32(record[146]),
			Zinc_100g:                            parseFloat32(record[147]),
			Copper_100g:                          parseFloat32(record[148]),
			Manganese_100g:                       parseFloat32(record[149]),
			Fluoride_100g:                        parseFloat32(record[150]),
			Selenium_100g:                        parseFloat32(record[151]),
			Chromium_100g:                        parseFloat32(record[152]),
			Molybdenum_100g:                      parseFloat32(record[153]),
			Iodine_100g:                          parseFloat32(record[154]),
			Caffeine_100g:                        parseFloat32(record[155]),
			Taurine_100g:                         parseFloat32(record[156]),
			Ph_100g:                              parseFloat32(record[157]),
			Fruits_vegetables_nuts_100g:          parseFloat32(record[158]),
			Fruits_vegetables_nuts_estimate_100g: parseFloat32(record[159]),
			Collagen_meat_protein_ratio_100g:     parseFloat32(record[160]),
			Cocoa_100g:                           parseFloat32(record[161]),
			Chlorophyl_100g:                      parseFloat32(record[162]),
			Carbon_footprint_100g:                parseFloat32(record[163]),
			Nutrition_score_fr_100g:              parseFloat32(record[164]),
			Nutrition_score_uk_100g:              parseFloat32(record[165]),
			Glycemic_index_100g:                  parseFloat32(record[166]),
			Water_hardness_100g:                  parseFloat32(record[167]),
			Choline_100g:                         parseFloat32(record[168]),
			Phylloquinone_100g:                   parseFloat32(record[169]),
			Beta_glucan_100g:                     parseFloat32(record[170]),
			Inositol_100g:                        parseFloat32(record[171]),
			Carnitine_100g:                       parseFloat32(record[172]),
		},
	}
	return
}

type Product struct {
	Code    string `json:"code"`
	Url     string `json:"url"`
	Creator string `json:"creator"`
	// Created_t                                  string    `json:"created_t"`
	Created_datetime time.Time `json:"created_datetime"`
	// Last_modified_t                            string    `json:"last_modified_t"`
	Last_modified_datetime                     time.Time `json:"last_modified_datetime"`
	Product_name                               string    `json:"product_name"`
	Generic_name                               string    `json:"generic_name"`
	Quantity                                   string    `json:"quantity"`
	Packaging                                  []string  `json:"packaging"`
	Packaging_tags                             []string  `json:"packaging_tags"`
	Brands                                     []string  `json:"brands"`
	Brands_tags                                []string  `json:"brands_tags"`
	Categories                                 []string  `json:"categories"`
	Categories_tags                            []string  `json:"categories_tags"`
	Categories_en                              []string  `json:"categories_en"`
	Origins                                    []string  `json:"origins"`
	Origins_tags                               []string  `json:"origins_tags"`
	Manufacturing_places                       []string  `json:"manufacturing_places"`
	Manufacturing_places_tags                  []string  `json:"manufacturing_places_tags"`
	Labels                                     []string  `json:"labels"`
	Labels_tags                                []string  `json:"labels_tags"`
	Labels_en                                  []string  `json:"labels_en"`
	Emb_codes                                  []string  `json:"emb_codes"`
	Emb_codes_tags                             []string  `json:"emb_codes_tags"`
	First_packaging_code_geo                   string    `json:"first_packaging_code_geo"`
	Cities                                     []string  `json:"cities"`
	Cities_tags                                []string  `json:"cities_tags"`
	Purchase_places                            []string  `json:"purchase_places"`
	Stores                                     []string  `json:"stores"`
	Countries                                  []string  `json:"countries"`
	Countries_tags                             []string  `json:"countries_tags"`
	Countries_en                               []string  `json:"countries_en"`
	Ingredients_text                           string    `json:"ingredients_text"`
	Allergens                                  string    `json:"allergens"`
	Allergens_en                               string    `json:"allergens_en"`
	Traces                                     []string  `json:"traces"`
	Traces_tags                                []string  `json:"traces_tags"`
	Traces_en                                  []string  `json:"traces_en"`
	Serving_size                               string    `json:"serving_size"`
	Serving_quantity                           float32   `json:"serving_quantity"`
	No_nutriments                              string    `json:"no_nutriments"`
	Additives_n                                int       `json:"additives_n"`
	Additives                                  []string  `json:"additives"`
	Additives_tags                             []string  `json:"additives_tags"`
	Additives_en                               []string  `json:"additives_en"`
	Ingredients_from_palm_oil_n                int       `json:"ingredients_from_palm_oil_n"`
	Ingredients_from_palm_oil                  string    `json:"ingredients_from_palm_oil"`
	Ingredients_from_palm_oil_tags             []string  `json:"ingredients_from_palm_oil_tags"`
	Ingredients_that_may_be_from_palm_oil_n    int       `json:"ingredients_that_may_be_from_palm_oil_n"`
	Ingredients_that_may_be_from_palm_oil      string    `json:"ingredients_that_may_be_from_palm_oil"`
	Ingredients_that_may_be_from_palm_oil_tags []string  `json:"ingredients_that_may_be_from_palm_oil_tags"`
	Nutrition_grade_uk                         string    `json:"nutrition_grade_uk"`
	Nutrition_grade_fr                         string    `json:"nutrition_grade_fr"`
	Pnns_groups_1                              string    `json:"pnns_groups_1"`
	Pnns_groups_2                              string    `json:"pnns_groups_2"`
	States                                     []string  `json:"states"`
	States_tags                                []string  `json:"states_tags"`
	States_en                                  []string  `json:"states_en"`
	Main_category                              []string  `json:"main_category"`
	Main_category_en                           []string  `json:"main_category_en"`
	Image_url                                  string    `json:"image_url"`
	Image_small_url                            string    `json:"image_small_url"`
	Image_ingredients_url                      string    `json:"image_ingredients_url"`
	Image_ingredients_small_url                string    `json:"image_ingredients_small_url"`
	Image_nutrition_url                        string    `json:"image_nutrition_url"`
	Image_nutrition_small_url                  string    `json:"image_nutrition_small_url"`
	Nutrition                                  Nutrition `json:"nutrition"`
}

type Nutrition struct {
	Energy_100g                          float32 `json:"energy_100g"`
	Energy_from_fat_100g                 float32 `json:"energy_from_fat_100g"`
	Fat_100g                             float32 `json:"fat_100g"`
	Saturated_fat_100g                   float32 `json:"saturated_fat_100g"`
	Butyric_acid_100g                    float32 `json:"butyric_acid_100g"`
	Caproic_acid_100g                    float32 `json:"caproic_acid_100g"`
	Caprylic_acid_100g                   float32 `json:"caprylic_acid_100g"`
	Capric_acid_100g                     float32 `json:"capric_acid_100g"`
	Lauric_acid_100g                     float32 `json:"lauric_acid_100g"`
	Myristic_acid_100g                   float32 `json:"myristic_acid_100g"`
	Palmitic_acid_100g                   float32 `json:"palmitic_acid_100g"`
	Stearic_acid_100g                    float32 `json:"stearic_acid_100g"`
	Arachidic_acid_100g                  float32 `json:"arachidic_acid_100g"`
	Behenic_acid_100g                    float32 `json:"behenic_acid_100g"`
	Lignoceric_acid_100g                 float32 `json:"lignoceric_acid_100g"`
	Cerotic_acid_100g                    float32 `json:"cerotic_acid_100g"`
	Montanic_acid_100g                   float32 `json:"Montanic_acid_100g"`
	Melissic_acid_100g                   float32 `json:"Melissic_acid_100g"`
	Monounsaturated_fat_100g             float32 `json:"monounsaturated_fat_100g"`
	Polyunsaturated_fat_100g             float32 `json:"polyunsaturated_fat_100g"`
	Omega_3_fat_100g                     float32 `json:"omega_3_fat_100g"`
	Alpha_linolenic_acid_100g            float32 `json:"alpha_linolenic_acid_100g"`
	Eicosapentaenoic_acid_100g           float32 `json:"eicosapentaenoic_acid_100g"`
	Docosahexaenoic_acid_100g            float32 `json:"docosahexaenoic_acid_100g"`
	Omega_6_fat_100g                     float32 `json:"omega_6_fat_100g"`
	Linoleic_acid_100g                   float32 `json:"linoleic_acid_100g"`
	Arachidonic_acid_100g                float32 `json:"arachidonic_acid_100g"`
	Gamma_linolenic_acid_100g            float32 `json:"gamma_linolenic_acid_100g"`
	Dihomo_gamma_linolenic_acid_100g     float32 `json:"dihomo_gamma_linolenic_acid_100g"`
	Omega_9_fat_100g                     float32 `json:"omega_9_fat_100g"`
	Oleic_acid_100g                      float32 `json:"oleic_acid_100g"`
	Elaidic_acid_100g                    float32 `json:"elaidic_acid_100g"`
	Gondoic_acid_100g                    float32 `json:"gondoic_acid_100g"`
	Mead_acid_100g                       float32 `json:"mead_acid_100g"`
	Erucic_acid_100g                     float32 `json:"erucic_acid_100g"`
	Nervonic_acid_100g                   float32 `json:"Nervonic_acid_100g"`
	Trans_fat_100g                       float32 `json:"trans_fat_100g"`
	Cholesterol_100g                     float32 `json:"cholesterol_100g"`
	Carbohydrates_100g                   float32 `json:"carbohydrates_100g"`
	Sugars_100g                          float32 `json:"sugars_100g"`
	Sucrose_100g                         float32 `json:"sucrose_100g"`
	Glucose_100g                         float32 `json:"glucose_100g"`
	Fructose_100g                        float32 `json:"fructose_100g"`
	Lactose_100g                         float32 `json:"lactose_100g"`
	Maltose_100g                         float32 `json:"maltose_100g"`
	Maltodextrins_100g                   float32 `json:"maltodextrins_100g"`
	Starch_100g                          float32 `json:"starch_100g"`
	Polyols_100g                         float32 `json:"polyols_100g"`
	Fiber_100g                           float32 `json:"fiber_100g"`
	Proteins_100g                        float32 `json:"proteins_100g"`
	Casein_100g                          float32 `json:"casein_100g"`
	Serum_proteins_100g                  float32 `json:"serum_proteins_100g"`
	Nucleotides_100g                     float32 `json:"nucleotides_100g"`
	Salt_100g                            float32 `json:"salt_100g"`
	Sodium_100g                          float32 `json:"sodium_100g"`
	Alcohol_100g                         float32 `json:"alcohol_100g"`
	Vitamin_a_100g                       float32 `json:"vitamin_a_100g"`
	Beta_carotene_100g                   float32 `json:"beta_carotene_100g"`
	Vitamin_d_100g                       float32 `json:"vitamin_d_100g"`
	Vitamin_e_100g                       float32 `json:"vitamin_e_100g"`
	Vitamin_k_100g                       float32 `json:"vitamin_k_100g"`
	Vitamin_c_100g                       float32 `json:"vitamin_c_100g"`
	Vitamin_b1_100g                      float32 `json:"vitamin_b1_100g"`
	Vitamin_b2_100g                      float32 `json:"vitamin_b2_100g"`
	Vitamin_pp_100g                      float32 `json:"vitamin_pp_100g"`
	Vitamin_b6_100g                      float32 `json:"vitamin_b6_100g"`
	Vitamin_b9_100g                      float32 `json:"vitamin_b9_100g"`
	Folates_100g                         float32 `json:"folates_100g"`
	Vitamin_b12_100g                     float32 `json:"vitamin_b12_100g"`
	Biotin_100g                          float32 `json:"biotin_100g"`
	Pantothenic_acid_100g                float32 `json:"pantothenic_acid_100g"`
	Silica_100g                          float32 `json:"silica_100g"`
	Bicarbonate_100g                     float32 `json:"bicarbonate_100g"`
	Potassium_100g                       float32 `json:"potassium_100g"`
	Chloride_100g                        float32 `json:"chloride_100g"`
	Calcium_100g                         float32 `json:"calcium_100g"`
	Phosphorus_100g                      float32 `json:"phosphorus_100g"`
	Iron_100g                            float32 `json:"iron_100g"`
	Magnesium_100g                       float32 `json:"magnesium_100g"`
	Zinc_100g                            float32 `json:"zinc_100g"`
	Copper_100g                          float32 `json:"copper_100g"`
	Manganese_100g                       float32 `json:"manganese_100g"`
	Fluoride_100g                        float32 `json:"fluoride_100g"`
	Selenium_100g                        float32 `json:"selenium_100g"`
	Chromium_100g                        float32 `json:"chromium_100g"`
	Molybdenum_100g                      float32 `json:"molybdenum_100g"`
	Iodine_100g                          float32 `json:"iodine_100g"`
	Caffeine_100g                        float32 `json:"caffeine_100g"`
	Taurine_100g                         float32 `json:"taurine_100g"`
	Ph_100g                              float32 `json:"ph_100g"`
	Fruits_vegetables_nuts_100g          float32 `json:"fruits_vegetables_nuts_100g"`
	Fruits_vegetables_nuts_estimate_100g float32 `json:"fruits_vegetables_nuts_estimate_100g"`
	Collagen_meat_protein_ratio_100g     float32 `json:"collagen_meat_protein_ratio_100g"`
	Cocoa_100g                           float32 `json:"cocoa_100g"`
	Chlorophyl_100g                      float32 `json:"chlorophyl_100g"`
	Carbon_footprint_100g                float32 `json:"carbon_footprint_100g"`
	Nutrition_score_fr_100g              float32 `json:"nutrition_score_fr_100g"`
	Nutrition_score_uk_100g              float32 `json:"nutrition_score_uk_100g"`
	Glycemic_index_100g                  float32 `json:"glycemic_index_100g"`
	Water_hardness_100g                  float32 `json:"water_hardness_100g"`
	Choline_100g                         float32 `json:"choline_100g"`
	Phylloquinone_100g                   float32 `json:"phylloquinone_100g"`
	Beta_glucan_100g                     float32 `json:"beta_glucan_100g"`
	Inositol_100g                        float32 `json:"inositol_100g"`
	Carnitine_100g                       float32 `json:"carnitine_100g"`
}

func splitFields(field string) []string {
	if field == "" {
		return []string{}
	}

	return strings.Split(field, ",")
}

func parseInt64(s string) int64 {
	if s == "" {
		return -1
	}
	n, err := strconv.ParseInt(s, 10, 64)
	check(err)
	return n
}

func parseInt(s string) int {
	return int(parseInt64(s))
}

func parseFloat32(s string) float32 {
	if s == "" {
		return -1.0
	}
	n, err := strconv.ParseFloat(s, 32)
	check(err)
	return float32(n)
}

func parseDatetime(s string) time.Time {
	t, err := time.Parse(time.RFC3339, s)
	check(err)
	return t
}
