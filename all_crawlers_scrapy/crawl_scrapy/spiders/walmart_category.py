import json
import datetime
from datetime import datetime
import scrapy
from scrapy.conf import settings
import re

from .setuserv_spider import SetuservSpider
from scrapy import Selector
from urllib.parse import urlparse



class WalmartCategorySpider(SetuservSpider):
    name = 'walmart-category-scraper'

    def __init__(self, mongo_host, mongo_port, mongo_db, mongo_collection, document_id, env):
        super().__init__(mongo_host, mongo_port, mongo_db, mongo_collection, document_id, self.name, env)
        self.logger.info("Walmart category Scraping starts")
        assert self.source == 'walmart_category'

    def start_requests(self):
        self.logger.info("Starting requests")
        print("inside walmart category scraper")
        for category_url, category_id in zip(self.start_urls, self.product_ids):

            # for page in range(1, 25+1):
                page =1
                print("page {} is scraping".format(page))
                next_page_url = category_url+f"&page={page}&affinityOverride=default"
                print("next_page_url is ", next_page_url)
                media_entity = {'url': next_page_url, 'id': category_id, "base_url": category_url}
                #media_entity = {'base_url': category_url, 'id': category_id, "next_url": category_url}
                print("media entity is ", media_entity)
                media_entity = {**media_entity, **self.media_entity_logs}
                print("country code is: ", urlparse(category_url).netloc.split('.')[2])
                country_code = urlparse(category_url).netloc.split('.')[2]
                print("before scrapy request")

                if country_code == "ca":
                    print("inside walmart ca category scraper")
                    media_entity = {'url': category_url, 'id': category_id}
                    media_entity = {**media_entity, **self.media_entity_logs}

                    yield scrapy.Request(url= self.get_ca_category_url(category_url, page),
                                         callback=self.parse_products_ca,
                                         errback=self.err, dont_filter=True,
                                         headers={('User-Agent', 'Mozilla/5.0')},
                                         meta={'media_entity': media_entity,
                                               'page': page})
                else:

                    yield scrapy.Request(url= next_page_url,#self.get_category_url(category_id, page),
                                         callback=self.parse_products_us,
                                         errback=self.err, dont_filter=True,
                                         meta={'media_entity': media_entity,
                                               'page': page})
                self.logger.info(f"Generating reviews for {category_url} and {category_id}")

#i[class="ld ld-ChevronRight pv1 blue"]

    def get_ca_category_url(self, category_url, page):
        #https://www.walmart.ca/api/bsp/browse?experience=whiteGM&lang=en&c=20135&p=2
        c = category_url.split('?')[0].split("-")[-1]
        print("c: ",c)
        print("category_url: {} and c: {}".format(category_url, c))
        print("final_url is: ")
        if len(list(category_url.split('?')))>1:
            print("inside if .....,")
            params = category_url.split('?')[1]
            print("params ", params)
            final_url = f'https://www.walmart.ca/api/bsp/browse?experience=whiteGM&c={c}&lang=en&p={page}&{params}'
        else:
            final_url = f'https://www.walmart.ca/api/bsp/browse?experience=whiteGM&c={c}&lang=en&p={page}'

        #print(f'https://www.walmart.ca/api/bsp/browse?experience=whiteGM&c={c}&lang=en&p={page}&{params}')
        #print("actual url is: ", 'https://www.walmart.ca/api/bsp/browse?experience=whiteGM&f=1019767&lang=en&c=20135&p=2&b=1')
        print("final_url is: ", final_url)
        return final_url #f'https://www.walmart.ca/api/bsp/browse?experience=whiteGM&f=1019767&lang=en&c=20135&p={page}&b=1'

        #return f'https://www.walmart.ca/api/bsp/browse?experience=whiteGM&c={c}&lang=en&p={page}&{params}'

    def parse_products_ca(self, response):
        print("inside parse_products method")

        media_entity = response.meta["media_entity"]
        page = response.meta["page"]
        category_url = media_entity["url"]
        category_id = media_entity["id"]

        print("scraping page ", page)
        #category_url = media_entity["url"]
        #next_page_url = media_entity["url"]
        #print("next_page_url is: ", next_page_url)
        #base_url = media_entity["base_url"]

        if 'Robot or human' in response.text or 'class="re-captcha"' in response.text or 'Please download one of the supported browsers to keep shopping' in response.text:
            print("Robot or human occured, retrying")
            yield scrapy.Request(url= self.get_ca_category_url(category_url, page),
                                 callback=self.parse_products_ca,
                                 errback=self.err, dont_filter=True,
                                 meta={'media_entity': media_entity,
                                       'page': page})
            return
        print("dumping response ")
        self.dump(response, "html")
        print("response text is ", response.text)

        response_dic = json.loads(response.text)

        if len(response_dic["items"]["productIds"]) == 0 and len(response_dic["items"]["productsToFetch"]) == 0:
            print("all pages scraped last page is ", page)
            return
        else:
            prod_ids_lst = response_dic["items"]["productIds"]
            print(len(prod_ids_lst))
            for prod_id_dic in response_dic["items"]["productsToFetch"]:
                prod_ids_lst.append(prod_id_dic['product_id'])
            print(prod_ids_lst)
            print("no. of products extracted are: ", len(prod_ids_lst))

            prod_url_prefix = 'https://www.walmart.ca/en/ip/'
            prod_urls_lst = []
            for prod_id in prod_ids_lst:
                prod_urls_lst.append(prod_url_prefix+str(prod_id))

            print("prod_urls_lst is : ", prod_urls_lst)
            for i in range(len(prod_urls_lst)):

                self.yield_category_details(category_url= category_url,
                                                  product_url= prod_urls_lst[i],
                                                  product_id = prod_ids_lst[i],
                                                  extra_info='',
                                                  page_no = page,
                                                  is_sponsored = ''
                                                  )

            page = page + 1
            print("scraping again and scraping at page = ", page)
            yield scrapy.Request(url= self.get_ca_category_url(category_url, page),
                                 callback=self.parse_products_ca,
                                 errback=self.err, dont_filter=True,
                                 meta={'media_entity': media_entity,
                                       'page': page})



        # response_dic = response.text
        # print("type of res", type(response_dic))
        # #object►items►productIds►0
        # product_id = response_dic["object"]["items"]["productIds"]
        # print("product_id dic is: ", product_id)




    def parse_products_us(self, response):
        print("inside parse_products method")

        media_entity = response.meta["media_entity"]
        page = response.meta["page"]
        print("scraping page ", page)
        #category_url = media_entity["url"]
        next_page_url = media_entity["url"]
        print("next_page_url is: ", next_page_url)
        base_url = media_entity["base_url"]
        category_id = media_entity["id"]
        if 'Robot or human' in response.text or 'class="re-captcha"' in response.text or 'Please download one of the supported browsers to keep shopping' in response.text:
            print("Robot or human occured, retrying")
            yield scrapy.Request(url= next_page_url,#self.get_category_url(category_id, page),
                                 callback=self.parse_products_us,
                                 errback=self.err, dont_filter=True,
                                 #headers=self.get_headers(self.get_category_url(category_id, page)),
                                 meta={'media_entity': media_entity,
                                       'page': page})
            return
        print("dumping response ")
        #self.dump(response, "html")
        print("response text is ", response.text)

        sub_response = response.css('script[id="__NEXT_DATA__"]').extract_first()
        split_data = re.split(r'''<script id="__NEXT_DATA__" type="application/json" .*">''',str(sub_response),1)
        response_data = split_data[1][:-9]
        response_dic = json.loads(response_data)

        for i in range(0,50):
            try:
                product_url = "https://www.walmart.com"+str(response_dic["props"]["pageProps"]["initialData"]["searchResult"]["itemStacks"][0]["items"][i]["canonicalUrl"])
                product_id = response_dic["props"]["pageProps"]["initialData"]["searchResult"]["itemStacks"][0]["items"][i]["usItemId"]
                is_sponsored = "Yes" if response_dic["props"]["pageProps"]["initialData"]["searchResult"]["itemStacks"][0]["items"][i]["isSponsoredFlag"] else "No"
                self.yield_category_details(category_url= next_page_url,
                                                  product_url= product_url,
                                                  product_id = product_id,
                                                  extra_info='',
                                                  page_no = page,
                                                  is_sponsored = is_sponsored
                                                  )
                print("i, page, product_url, product_id, is_sponsored are : ",i, page, product_url, product_id, is_sponsored)
            except Exception as e:
                print("actual error is ", e)
                print("index error i think at i=", i)
                continue

        # is_next_page_exists = response.css('i[class="ld ld-ChevronRight pv1 blue"]').extract()
        # print("is_next_page_exists", is_next_page_exists)
        total_pages = response_dic["props"]["pageProps"]["initialData"]["searchResult"]["paginationV2"]["maxPage"]
        print("total pages are ", total_pages)

        page = page+1
        if(page< total_pages):
            next_page_url = media_entity["base_url"]+f"&page={page}&affinityOverride=default"
            media_entity["url"] = next_page_url
            yield scrapy.Request(url= next_page_url,#self.get_category_url(category_id, page),
                                 callback=self.parse_products_us,
                                 errback=self.err, dont_filter=True,
                                 meta={'media_entity': media_entity,
                                       'page': page})

        else:
            print("all pages scraped and last page is ", page)



        return


        #page= page+1

        #total_pages = response.css('div[class="sans-serif ph1 pv2 w4 h4 lh-copy border-box br-100 b--solid mh2-m db tc no-underline gray bg-white b--white-90"]::text').extract_first()
        #print("total pages are ", total_pages)
        # for page in range(2, 25):
        #     print("page no is ", page)
        #     next_page_url = category_url+f"&page={page}&affinityOverride=default"
        #     print("next page url is ", next_page_url)
        #
        #     yield scrapy.Request(url= next_page_url,#f"&page={page}&affinityOverride=default",#self.get_category_url(category_id, page),
        #                          callback=self.parse_products,
        #                          errback=self.err, dont_filter=True,
        #                          #headers=self.get_headers(self.get_category_url(category_id, page)),
        #                          meta={'media_entity': media_entity,
        #                                'page': page})

        #https://www.walmart.com/browse/cell-phones/all-cell-phones/1105910_5296594?povid=web_categorypage_cellphones_lefthandnav&page=3&affinityOverride=default








        #res = json.loads(response.text)
        # response_products_lst = response.css('div[class="mb1 ph1 pa0-xl bb b--near-white w-25"]')
        # print("type ", type(response_products_lst))
        # print("length is:")
        # print(len(response_products_lst))
        #
        # for product in response_products_lst:
        #     print("insdide loop:")
        #     print(product)
        #     #product_css = Selector(text=str(response))
        #     product_css = product
        #     print(type(response), type(product_css))
        #     prod_id = product_css.css('div::attr(data-item-id)').extract()
        #     prod_link = product_css.css('a::attr(href)').extract()
        #     sponsored = "True" if len(product_css.css('span[class="gray f7"]::text').extract())==1 else "False"#len(product_css.css('span[class="gray f7"]::text').extract())
        #     print("prod_id and prod_link, sponsored are ", prod_id, prod_link, sponsored)
        #     print(product_css.css('div#2CUKLPB52D6D'))
        #     print(product_css.css('div["data-item-id"="2CUKLPB52D6D"]').extract())



    # def start_requests(self):
    #     self.logger.info("Starting requests")
    #     print("inside walmart category scraper")
    #     for category_url, category_id in zip(self.start_urls, self.product_ids):
    #         media_entity = {'url': category_url, 'id': category_id}
    #         #media_entity = {'base_url': category_url, 'id': category_id, "next_url": category_url}
    #         print("media entity is ", media_entity)
    #         media_entity = {**media_entity, **self.media_entity_logs}
    #         page = 1
    #         print("before scrapy request")
    #         yield scrapy.Request(url= category_url,#self.get_category_url(category_id, page),
    #                              callback=self.parse_products,
    #                              errback=self.err, dont_filter=True,
    #                              meta={'media_entity': media_entity,
    #                                    'page': page})
    #
    #         # yield scrapy.Request(url= self.get_category_url(page),#self.get_category_url(category_id, page),
    #         #                      callback=self.parse_products,
    #         #                      errback=self.err, dont_filter=True,
    #         #                      meta={'media_entity': media_entity,
    #         #                            'page': page},
    #         #                     method = 'POST',
    #         #                     body=json.dumps(self.get_payload(page)),
    #         #                     headers=self.get_headers(self.get_category_url(page)))
    #
    #         self.logger.info(f"Generating reviews for {category_url} and {category_id}")

    # def parse_products(self, response):
    #     print("inside parse_products method")
    #     media_entity = response.meta["media_entity"]
    #     page = response.meta["page"]
    #     category_url = media_entity["url"]
    #     category_id = media_entity["id"]
    #     if 'Robot or human' in response.text or 'class="re-captcha"' in response.text:
    #         print("Robot or human occured, retrying")
    #         yield scrapy.Request(url= category_url,#self.get_category_url(category_id, page),
    #                              callback=self.parse_products,
    #                              errback=self.err, dont_filter=True,
    #                              headers=self.get_headers(self.get_category_url(page)),
    #                              meta={'media_entity': media_entity,
    #                                    'page': page})
    #         return
    #     print("dumping response ")
    #     self.dump(response, "html")
    #     print("response text is ", response.text)


    # def get_category_url(self, page):
    #     print("inside get_category_url")
    #     url = f'https://www.walmart.com/orchestra/home/graphql/browse?affinityOverride=default&page={page}&prg=desktop&catId=1105910_5296594&sort=best_match&ps=40&additionalQueryParams.isMoreOptionsTileEnabled=True&searchArgs.cat_id=1105910_5296594&searchArgs.prg=desktop&fitmentFieldParams=True&enableFashionTopNav=false&fetchMarquee=True&fetchSkyline=True&fetchSbaTop=false&fetchGallery=false&enablePortableFacets=True&tenant=WM_GLASS&enableFacetCount=True&marketSpecificParams=undefined&enableFlattenedFitment=false'
    #     return url
    # def get_payload(self, page):
    #     print("inside get_payload method")
    #     payload_dic = {"query":"query Browse( $query:String $page:Int $prg:Prg! $facet:String $sort:Sort $catId:String! $max_price:String $min_price:String $module_search:String $affinityOverride:AffinityOverride $ps:Int $ptss:String $beShelfId:String $fitmentFieldParams:JSON ={}$fitmentSearchParams:JSON ={}$rawFacet:String $seoPath:String $trsp:String $fetchMarquee:Boolean! $fetchSkyline:Boolean! $fetchGallery:Boolean! $additionalQueryParams:JSON ={}$enablePortableFacets:Boolean = false $enableFashionTopNav:Boolean = false $intentSource:IntentSource $tenant:String! $enableFacetCount:Boolean = True $pageType:String! = \"BrowsePage\" $marketSpecificParams:String $enableFlattenedFitment:Boolean = false ){search( query:$query page:$page prg:$prg facet:$facet sort:$sort cat_id:$catId max_price:$max_price min_price:$min_price module_search:$module_search affinityOverride:$affinityOverride additionalQueryParams:$additionalQueryParams ps:$ps ptss:$ptss trsp:$trsp intentSource:$intentSource _be_shelf_id:$beShelfId pageType:$pageType ){query searchResult{...BrowseResultFragment}}contentLayout( channel:\"WWW\" pageType:$pageType tenant:$tenant version:\"v1\" searchArgs:{query:$query cat_id:$catId _be_shelf_id:$beShelfId prg:$prg}){modules{...ModuleFragment configs{...on EnricherModuleConfigsV1{zoneV1}__typename...on _TempoWM_GLASSWWWSearchSortFilterModuleConfigs{facetsV1 @skip(if:$enablePortableFacets){...FacetFragment}topNavFacets @include(if:$enablePortableFacets){...FacetFragment}allSortAndFilterFacets @include(if:$enablePortableFacets){...FacetFragment}}...on TempoWM_GLASSWWWPillsModuleConfigs{moduleSource pillsV2{...PillsModuleFragment}}...TileTakeOverProductFragment...on TempoWM_GLASSWWWSearchFitmentModuleConfigs{fitments( fitmentSearchParams:$fitmentSearchParams fitmentFieldParams:$fitmentFieldParams ){...FitmentFragment sisFitmentResponse{...BrowseResultFragment}}}...on TempoWM_GLASSWWWStoreSelectionHeaderConfigs{fulfillmentMethodLabel storeDislayName}...on TempoWM_GLASSWWWSponsoredProductCarouselConfigs{_rawConfigs}...FashionTopNavFragment @include(if:$enableFashionTopNav)...PopularInModuleFragment...CopyBlockModuleFragment...BannerModuleFragment...HeroPOVModuleFragment...InlineSearchModuleFragment...MarqueeDisplayAdConfigsFragment @include(if:$fetchMarquee)...SkylineDisplayAdConfigsFragment @include(if:$fetchSkyline)...GalleryDisplayAdConfigsFragment @include(if:$fetchGallery)...HorizontalChipModuleConfigsFragment...SkinnyBannerFragment}}...LayoutFragment pageMetadata{location{pickupStore deliveryStore intent postalCode stateOrProvinceCode city storeId accessPointId accessType spokeNodeId}pageContext}}seoBrowseMetaData( id:$catId facets:$rawFacet path:$seoPath facet_query_param:$facet _be_shelf_id:$beShelfId marketSpecificParams:$marketSpecificParams ){metaTitle metaDesc metaCanon h1 noIndex}}fragment BrowseResultFragment on SearchInterface{title aggregatedCount...BreadCrumbFragment...ShelfDataFragment...DebugFragment...ItemStacksFragment...PageMetaDataFragment...PaginationFragment...RequestContextFragment...ErrorResponse modules{facetsV1 @skip(if:$enablePortableFacets){...FacetFragment}topNavFacets @include(if:$enablePortableFacets){...FacetFragment}allSortAndFilterFacets @include(if:$enablePortableFacets){...FacetFragment}pills{...PillsModuleFragment}}pac{relevantPT{productType score}showPAC reasonCode}}fragment ModuleFragment on TempoModule{name version type moduleId schedule{priority}matchedTrigger{zone}}fragment LayoutFragment on ContentLayout{layouts{id layout}}fragment BreadCrumbFragment on SearchInterface{breadCrumb{id name url}}fragment ShelfDataFragment on SearchInterface{shelfData{shelfName shelfId}}fragment DebugFragment on SearchInterface{debug{sisUrl adsUrl}}fragment ItemStacksFragment on SearchInterface{itemStacks{displayMessage meta{adsBeacon{adUuid moduleInfo max_ads}query stackId stackType title layoutEnum totalItemCount totalItemCountDisplay viewAllParams{query cat_id sort facet affinityOverride recall_set min_price max_price}}itemsV2{...ItemFragment...InGridMarqueeAdFragment...TileTakeOverTileFragment}}}fragment ItemFragment on Product{__typename id usItemId fitmentLabel name checkStoreAvailabilityATC seeShippingEligibility brand type shortDescription weightIncrement imageInfo{...ProductImageInfoFragment}canonicalUrl externalInfo{url}itemType category{path{name url}}badges{flags{...on BaseBadge{key text type id}...on PreviouslyPurchasedBadge{id text key lastBoughtOn numBought}}tags{...on BaseBadge{key text type}}}classType averageRating numberOfReviews esrb mediaRating salesUnitType sellerId sellerName hasSellerBadge isEarlyAccessItem earlyAccessEvent annualEvent availabilityStatusV2{display value}groupMetaData{groupType groupSubType numberOfComponents groupComponents{quantity offerId componentType productDisplayName}}productLocation{displayValue aisle{zone aisle}}fulfillmentSpeed offerId preOrder{...PreorderFragment}pac{showPAC reasonCode}priceInfo{...ProductPriceInfoFragment}variantCriteria{...VariantCriteriaFragment}snapEligible fulfillmentBadge fulfillmentTitle fulfillmentType brand manufacturerName showAtc sponsoredProduct{spQs clickBeacon spTags viewBeacon}showOptions showBuyNow quickShop rewards{eligible state minQuantity rewardAmt promotionId selectionToken cbOffer term expiry description}arExperiences{isARHome isZeekit}eventAttributes{...ProductEventAttributesFragment}subscription{subscriptionEligible}}fragment ProductImageInfoFragment on ProductImageInfo{thumbnailUrl size}fragment ProductPriceInfoFragment on ProductPriceInfo{priceRange{minPrice maxPrice priceString}currentPrice{...ProductPriceFragment priceDisplay}comparisonPrice{...ProductPriceFragment}wasPrice{...ProductPriceFragment}unitPrice{...ProductPriceFragment}listPrice{...ProductPriceFragment}savingsAmount{...ProductSavingsFragment}shipPrice{...ProductPriceFragment}subscriptionPrice{priceString subscriptionString}priceDisplayCodes{priceDisplayCondition finalCostByWeight submapType}wPlusEarlyAccessPrice{memberPrice{...ProductPriceFragment}savings{...ProductSavingsFragment}eventStartTime eventStartTimeDisplay}}fragment PreorderFragment on PreOrder{isPreOrder preOrderMessage preOrderStreetDateMessage}fragment ProductPriceFragment on ProductPrice{price priceString variantPriceString priceType currencyUnit priceDisplay}fragment ProductSavingsFragment on ProductSavings{amount percent priceString}fragment ProductEventAttributesFragment on EventAttributes{priceFlip specialBuy}fragment VariantCriteriaFragment on VariantCriterion{name type id isVariantTypeSwatch variantList{id images name rank swatchImageUrl availabilityStatus products selectedProduct{canonicalUrl usItemId}}}fragment InGridMarqueeAdFragment on MarqueePlaceholder{__typename type moduleLocation lazy}fragment TileTakeOverTileFragment on TileTakeOverProductPlaceholder{__typename type tileTakeOverTile{span title subtitle image{src alt}logoImage{src alt}backgroundColor titleTextColor subtitleTextColor tileCta{ctaLink{clickThrough{value}linkText title}ctaType ctaTextColor}}}fragment PageMetaDataFragment on SearchInterface{pageMetadata{storeSelectionHeader{fulfillmentMethodLabel storeDislayName}title canonical description location{addressId}}}fragment PaginationFragment on SearchInterface{paginationV2{maxPage pageProperties}}fragment RequestContextFragment on SearchInterface{requestContext{vertical isFitmentFilterQueryApplied searchMatchType categories{id name}}}fragment ErrorResponse on SearchInterface{errorResponse{correlationId source errorCodes errors{errorType statusCode statusMsg source}}}fragment PillsModuleFragment on PillsSearchInterface{title url image:imageV1{src alt}}fragment BannerViewConfigFragment on BannerViewConfigCLS{title image imageAlt displayName description url urlAlt appStoreLink appStoreLinkAlt playStoreLink playStoreLinkAlt}fragment BannerModuleFragment on TempoWM_GLASSWWWSearchBannerConfigs{moduleType viewConfig{...BannerViewConfigFragment}}fragment PopularInModuleFragment on TempoWM_GLASSWWWPopularInBrowseConfigs{seoBrowseRelmData(id:$catId){relm{id name url}}}fragment CopyBlockModuleFragment on TempoWM_GLASSWWWCopyBlockConfigs{copyBlock(id:$catId marketSpecificParams:$marketSpecificParams){cwc}}fragment FacetFragment on Facet{title name expandOnLoad type layout min max selectedMin selectedMax unboundedMax stepSize isSelected values{id title name expandOnLoad description type itemCount @include(if:$enableFacetCount) isSelected baseSeoURL values{id title name expandOnLoad description type itemCount @include(if:$enableFacetCount) isSelected baseSeoURL values{id title name expandOnLoad description type itemCount @include(if:$enableFacetCount) isSelected baseSeoURL values{id title name expandOnLoad description type itemCount @include(if:$enableFacetCount) isSelected baseSeoURL values{id title name expandOnLoad description type itemCount @include(if:$enableFacetCount) isSelected baseSeoURL values{id title name expandOnLoad description type itemCount @include(if:$enableFacetCount) isSelected baseSeoURL values{id title name expandOnLoad description type itemCount @include(if:$enableFacetCount) isSelected baseSeoURL values{id title name expandOnLoad description type itemCount @include(if:$enableFacetCount) isSelected baseSeoURL}}}}}}}}}fragment FitmentFragment on Fitments{partTypeIDs result{status formId position quantityTitle extendedAttributes{...FitmentFieldFragment}labels{...LabelFragment}resultSubTitle notes suggestions{...FitmentSuggestionFragment}}labels{...LabelFragment}savedVehicle{vehicleType{...VehicleFieldFragment}vehicleYear{...VehicleFieldFragment}vehicleMake{...VehicleFieldFragment}vehicleModel{...VehicleFieldFragment}additionalAttributes{...VehicleFieldFragment}}fitmentFields{...VehicleFieldFragment}fitmentForms{id fields{...FitmentFieldFragment}title labels{...LabelFragment}}}fragment LabelFragment on FitmentLabels{ctas{...FitmentLabelEntityFragment}messages{...FitmentLabelEntityFragment}links{...FitmentLabelEntityFragment}images{...FitmentLabelEntityFragment}}fragment FitmentLabelEntityFragment on FitmentLabelEntity{id label labelV1 @include(if:$enableFlattenedFitment)}fragment VehicleFieldFragment on FitmentVehicleField{id label value}fragment FitmentFieldFragment on FitmentField{id displayName value extended data{value label}dependsOn}fragment FitmentSuggestionFragment on FitmentSuggestion{id position loadIndex speedRating searchQueryParam labels{...LabelFragment}cat_id fitmentSuggestionParams{id value}}fragment HeroPOVModuleFragment on TempoWM_GLASSWWWHeroPovConfigsV1{povCards{card{povStyle image{mobileImage{...TempoCommonImageFragment}desktopImage{...TempoCommonImageFragment}}heading{text textColor textSize}subheading{text textColor}detailsView{backgroundColor isTransparent}ctaButton{button{linkText clickThrough{value}uid}}legalDisclosure{regularText shortenedText textColor textColorMobile legalBottomSheetTitle legalBottomSheetDescription}logo{...TempoCommonImageFragment}links{link{linkText}}}}}fragment TempoCommonImageFragment on TempoCommonImage{src alt assetId uid clickThrough{value}}fragment InlineSearchModuleFragment on TempoWM_GLASSWWWInlineSearchConfigs{headingText placeholderText}fragment MarqueeDisplayAdConfigsFragment on TempoWM_GLASSWWWMarqueeDisplayAdConfigs{_rawConfigs ad{...DisplayAdFragment}}fragment DisplayAdFragment on Ad{...AdFragment adContent{type data{__typename...AdDataDisplayAdFragment}}}fragment AdFragment on Ad{status moduleType platform pageId pageType storeId stateCode zipCode pageContext moduleConfigs adsContext adRequestComposite}fragment AdDataDisplayAdFragment on AdData{...on DisplayAd{json status}}fragment SkylineDisplayAdConfigsFragment on TempoWM_GLASSWWWSkylineDisplayAdConfigs{_rawConfigs ad{...SkylineDisplayAdFragment}}fragment SkylineDisplayAdFragment on Ad{...SkylineAdFragment adContent{type data{__typename...SkylineAdDataDisplayAdFragment}}}fragment SkylineAdFragment on Ad{status moduleType platform pageId pageType storeId stateCode zipCode pageContext moduleConfigs adsContext adRequestComposite}fragment SkylineAdDataDisplayAdFragment on AdData{...on DisplayAd{json status}}fragment GalleryDisplayAdConfigsFragment on TempoWM_GLASSWWWGalleryDisplayAdConfigs{_rawConfigs}fragment HorizontalChipModuleConfigsFragment on TempoWM_GLASSWWWHorizontalChipModuleConfigs{chipModuleSource:moduleSource chipModule{title url{linkText title clickThrough{type value}}}chipModuleWithImages{title url{linkText title clickThrough{type value}}image{alt clickThrough{type value}height src title width}}}fragment SkinnyBannerFragment on TempoWM_GLASSWWWSkinnyBannerConfigs{bannerType desktopBannerHeight bannerImage{src title alt}mobileBannerHeight mobileImage{src title alt}clickThroughUrl{clickThrough{value}}backgroundColor heading{title fontColor}subHeading{title fontColor}bannerCta{ctaLink{linkText clickThrough{value}}textColor ctaType}}fragment TileTakeOverProductFragment on TempoWM_GLASSWWWTileTakeOverProductConfigs{dwebSlots mwebSlots TileTakeOverProductDetails{pageNumber span dwebPosition mwebPosition title subtitle image{src alt}logoImage{src alt}backgroundColor titleTextColor subtitleTextColor tileCta{ctaLink{clickThrough{value}linkText title}ctaType ctaTextColor}}}fragment FashionTopNavFragment on TempoWM_GLASSWWWCategoryTopNavConfigs{navHeaders{header{linkText clickThrough{value}}headerImageGroup{headerImage{alt src}imgTitle imgSubText imgLink{linkText title clickThrough{value}}}categoryGroup{category{linkText clickThrough{value}}startNewColumn subCategoryGroup{subCategory{linkText clickThrough{value}}isBold openInNewTab}}}}","variables":{"id":"","affinityOverride":"default","dealsId":"","query":"","page":page,"prg":"desktop","catId":"1105910_5296594","facet":"","sort":"best_match","rawFacet":"","seoPath":"","ps":40,"ptss":"","trsp":"","beShelfId":"","recall_set":"","module_search":"","min_price":"","max_price":"","storeSlotBooked":"","additionalQueryParams":{"hidden_facet":"null","translation":"null","isMoreOptionsTileEnabled":True},"searchArgs":{"query":"","cat_id":"1105910_5296594","prg":"desktop","facet":""},"fitmentFieldParams":{"powerSportEnabled":True},"fitmentSearchParams":{"id":"","affinityOverride":"default","dealsId":"","query":"","page":page,"prg":"desktop","catId":"1105910_5296594","facet":"","sort":"best_match","rawFacet":"","seoPath":"","ps":40,"ptss":"","trsp":"","beShelfId":"","recall_set":"","module_search":"","min_price":"","max_price":"","storeSlotBooked":"","additionalQueryParams":{"hidden_facet":"null","translation":"null","isMoreOptionsTileEnabled":True},"searchArgs":{"query":"","cat_id":"1105910_5296594","prg":"desktop","facet":""},"cat_id":"1105910_5296594","_be_shelf_id":""},"enableFashionTopNav":False,"fetchMarquee":True,"fetchSkyline":True,"fetchSbaTop":False,"fetchGallery":False,"enablePortableFacets":True,"tenant":"WM_GLASS","enableFacetCount":True,"enableFlattenedFitment":False,"pageType":"BrowsePage"}}
    #     return payload_dic

    # def get_headers(self, product_url):
    #     print("inside get_headers method")
    #     # headers = {
    #     #     'authority': 'www.walmart.com',
    #     #     'scheme': 'https',
    #     #     'accept': 'application/json',
    #     #     'accept-encoding': 'gzip, deflate, br',
    #     #     'accept-language': 'en-US,en;q=0.9',
    #     #     'path': product_url.split('https://www.walmart.com')[1],
    #     #     'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/105.0.0.0 Safari/537.36',
    #     #     'x-apollo-operation-name': 'Browse'
    #     #     #'x-api-source': 'pc',
    #     #     # 'x-requested-with': 'XMLHttpRequest',
    #     #     # 'x-shopee-language': 'id'
    #     # }
    #     headers_new = {'authority': 'www.walmart.com',
    #                     'method': 'POST',
    #                     'path': '/orchestra/home/graphql/browse?affinityOverride=default&page=1&prg=desktop&catId=1105910_5296594&sort=best_match&ps=40&additionalQueryParams.isMoreOptionsTileEnabled=true&searchArgs.cat_id=1105910_5296594&searchArgs.prg=desktop&fitmentFieldParams=true&enableFashionTopNav=false&fetchMarquee=true&fetchSkyline=true&fetchSbaTop=false&fetchGallery=false&enablePortableFacets=true&tenant=WM_GLASS&enableFacetCount=true&marketSpecificParams=undefined&enableFlattenedFitment=false',
    #                     'scheme': 'https',
    #                     'accept': 'application/json',
    #                     'accept-encoding': 'gzip, deflate, br',
    #                     'accept-language': 'en-US,en;q=0.9',
    #                     'content-length': '14881',
    #                     'content-type': 'application/json',
    #                     'cookie': 'dimensionData=821; TBV=7; _pxvid=3ad9e223-2d19-11ed-b1fe-5a6b6b704441; ACID=a5f04246-d86e-4a11-8bff-59c01ee325fa; hasACID=true; _pxhd=c38ee6c3b3d4993c4bf2c0abf3305f1173fb894504fbc0b492fedf044f7faea1:3ad1df4d-2d19-11ed-b56b-5a4d65587a71; vtc=Tq-62mkGRhEj4oS1JBSPSs; AID=wmlspartner%3D0%3Areflectorid%3D0000000000000000000000%3Alastupd%3D1662382210855; pxcts=26df1d84-389f-11ed-b00b-6f526e6d6256; wmlh=d6639d97b78a768784938804d3774d37e1f3721ba1194a7137670d9694cae07d; locGuestData=eyJpbnRlbnQiOiJQSUNLVVAiLCJpc0V4cGxpY2l0IjpmYWxzZSwic3RvcmVJbnRlbnQiOiJQSUNLVVAiLCJtZXJnZUZsYWciOnRydWUsImlzRGVmYXVsdGVkIjpmYWxzZSwicGlja3VwIjp7Im5vZGVJZCI6IjE5OTgiLCJ0aW1lc3RhbXAiOjE2NjM2NjkwMDIxMzN9LCJwb3N0YWxDb2RlIjp7InRpbWVzdGFtcCI6MTY2MzY2OTAwMjEzMywiYmFzZSI6IjYwMjAxIn0sIm1wIjpbXSwidmFsaWRhdGVLZXkiOiJwcm9kOnYyOmE1ZjA0MjQ2LWQ4NmUtNGExMS04YmZmLTU5YzAxZWUzMjVmYSJ9; _abck=dfak4cixsfdm51kebm24_1827; adblocked=false; TB_SFOU-100=; _sp_id.ad94=466d8e13-eec5-42d9-af82-b7c53544d9f6.1663899663.2.1664174500.1663899663.94df57e4-f9a2-49b4-88ac-e2fa165149d9; assortmentStoreId=1998; hasLocData=1; bm_mi=1F4A14BD7F6E764260D1793B1EAA0860~YAAQfTYauC0msECDAQAAi/ROfRH1jL1oJJOCK6UoNpp8njAQCEsq0PUbaT+Lb2h7k2WImpXR+z4jiB2MXaYhijxXM5Z65LveB202P+jCF6UlyOhf/pGyUXANKhFr2MctWqWo2qurEWbPjCvztFV5LqHFJtgg9PKmrDsSWqKf7t2fEozgoL4CAxcKtk2DfnIwon0FZ8NaGli4GOp8CqQ4tmOwkEdP1rBj2D0qCYnlRzP0VaUHXR3Jbl5cYxfcQTS3QjlJXevAmbxrIJu9da3Qbm4d10Vogtg3Wm60CqRIEXtwhtcEZKTpKGtZdLANJJTqwfNGM50d7x4uH2q39tWV9SkHsvoqrpQuFlK8KAxTbkTAHm6DlcRYNitkz5IEal+JgA==~1; _astc=5b8cced966cc55aac1a34f0db392a209; ak_bmsc=0EA6990FD66677EF9D168B16D2AB3CE0~000000000000000000000000000000~YAAQfTYauKUmsECDAQAA6A9PfRGQ9B9LN0+t76ehmNihsqioNQtESPg4WohC8ZmeX/ZO/zcXv0+LCQlIwg6antk83skqas5FdIZUrjNZi9ONOtFbOOSKFjPkPf1UIsUZ0H27XHA5LF2ROSG+CGR6jtd+vf+wV+ZuZJBq7TE8ogLOUp0ktiHgSw2LIpemaZ6dHu95FPe0Ge6pIhYq6liCOGWu0OPxCwBVNlFHxBVHzHHifF+ubhD5YiJwF8RsMk7hO4qWml3DbX8CJQsbyW/rdTVRlHFKGP3xvrXP/ypciIG60Ll5vh9qtHlCdoUi9PTGGEVS5IXNiCrjOIbINV3ZSvMDPI7KHJFEglALW9rjxCsg+g6zaVqv05+NhXwgz5d08nE3l5L2klbHE4FxclSwptCXtUEd4uvqPwkyAxrAKLp7hObLQveM3VSrDp3K/RoyaKqFNICNHj7ufcUTIedpht1s25O9/P9WfHUebjkHC0w=; bstc=UeW7ay_xADiJwUemwAtaos; mobileweb=0; xptc=assortmentStoreId%2B1998; xpth=x-o-mverified%2Bfalse; xpa=-lg_Y|0gx-S|0t4gT|20yr2|2J0Sx|2SWkj|4hJ7q|5q86Y|6r4vW|7LSfW|9T1D1|9ZpcO|9e61f|Au10N|B4NJZ|BrNRv|CVef3|DAgi2|DuuJN|GcXzr|GkqrP|H9VcM|I9lIr|JYP3v|MJuLK|MQ6mX|MXCqZ|N8QWO|Nnski|OMi8D|Qx0BC|Rgq78|SqhTR|Twnv9|U2PdV|Uq_L_|V-nnO|X8s-9|XcRMg|Y9_n0|YNNdy|a0tqZ|bEqes|bj38K|ccDGr|ckJk2|d1meI|d93_H|dth3N|eTJXg|fXIW_|h6I4g|jUBOl|lwUOE|sKR5M|sbguX|szDEg|tWk_I|tzVuW|u8ztl|uccoC|uj-23|wX6Ex|w_GEw|wexEY|zCylr|zFeZ5|zhSyo; exp-ck=0gx-S10t4gT120yr212J0Sx12SWkj14hJ7q27LSfW19ZpcO19e61f1Au10N1CVef31DAgi21GcXzr1JYP3v1MJuLK1MQ6mX1MXCqZ1Nnski1OMi8D2Qx0BC1Rgq781U2PdV1V-nnO1XcRMg3YNNdy1a0tqZ1bEqes1bj38K1ccDGr1ckJk21d1meI1dth3N1eTJXg2jUBOl1lwUOE1sbguX1szDEg1tWk_I1u8ztl1uccoC2w_GEw1wexEY4zFeZ51; TB_Latency_Tracker_100=1; TB_Navigation_Preload_01=1; xpm=1%2B1664258256%2BTq-62mkGRhEj4oS1JBSPSs~%2B0; auth=MTAyOTYyMDE4IHkOYnnPHW79XI2ctrSBb%2BUFqHn5dAY%2FwyPuNDrx6ajElpFTeNTGkd8GqbRYBPClierRnz96OYfFrBGE3KkVLh0CargoFCgQP%2F5GhUn3CSvT8raUcqyPgBAWziJNP%2Bh4767wuZloTfhm7Wk2KcjygpySosImygUk1x1iKsdnk4%2FzrSJrUn5nf8MBgxHhP%2BxydfS4pq0m3XcXWRP2K0b0JdnPx1DoxEMO98OZaeuo7msUMk70P8glgOEpLOprhDfMM%2FFHGZ2dCNmxWrdkwqEKrk6gSzzmVgRN%2FAQbzBZwtxQ%2FCE6DJskVl1IjZY2PAnEamVQ6m4PGBYaS2X0hgrCcDdG1ZIBdjYeF1q6nmB5OQl%2BwYvr2rUQlPIAmow3xZPHWS0K2Jq0NxWzJmH6JFjyEK0jyrOXbKKhH072NS%2FW0j%2FU%3D; locDataV3=eyJpc0RlZmF1bHRlZCI6ZmFsc2UsImlzRXhwbGljaXQiOmZhbHNlLCJpbnRlbnQiOiJQSUNLVVAiLCJwaWNrdXAiOlt7ImJ1SWQiOiIwIiwibm9kZUlkIjoiMTk5OCIsImRpc3BsYXlOYW1lIjoiU2tva2llIFN1cGVyY2VudGVyIiwibm9kZVR5cGUiOiJTVE9SRSIsImFkZHJlc3MiOnsicG9zdGFsQ29kZSI6IjYwMDc2IiwiYWRkcmVzc0xpbmUxIjoiMzYyNiBUb3VoeSBBdmUiLCJjaXR5IjoiU2tva2llIiwic3RhdGUiOiJJTCIsImNvdW50cnkiOiJVUyIsInBvc3RhbENvZGU5IjoiNjAwNzYtMzk0MyJ9LCJnZW9Qb2ludCI6eyJsYXRpdHVkZSI6NDIuMDEyODM0LCJsb25naXR1ZGUiOi04Ny43MTkxNDV9LCJpc0dsYXNzRW5hYmxlZCI6dHJ1ZSwic2NoZWR1bGVkRW5hYmxlZCI6dHJ1ZSwidW5TY2hlZHVsZWRFbmFibGVkIjp0cnVlLCJodWJOb2RlSWQiOiIxOTk4Iiwic3RvcmVIcnMiOiIwNjowMC0yMzowMCIsInN1cHBvcnRlZEFjY2Vzc1R5cGVzIjpbIlBJQ0tVUF9DVVJCU0lERSIsIlBJQ0tVUF9JTlNUT1JFIl19XSwic2hpcHBpbmdBZGRyZXNzIjp7ImxhdGl0dWRlIjo0Mi4wNTYzLCJsb25naXR1ZGUiOi04Ny42OTg2LCJwb3N0YWxDb2RlIjoiNjAyMDEiLCJjaXR5IjoiRXZhbnN0b24iLCJzdGF0ZSI6IklMIiwiY291bnRyeUNvZGUiOiJVU0EiLCJnaWZ0QWRkcmVzcyI6ZmFsc2V9LCJhc3NvcnRtZW50Ijp7Im5vZGVJZCI6IjE5OTgiLCJkaXNwbGF5TmFtZSI6IlNrb2tpZSBTdXBlcmNlbnRlciIsImFjY2Vzc1BvaW50cyI6bnVsbCwic3VwcG9ydGVkQWNjZXNzVHlwZXMiOltdLCJpbnRlbnQiOiJQSUNLVVAiLCJzY2hlZHVsZUVuYWJsZWQiOmZhbHNlfSwiZGVsaXZlcnkiOnsiYnVJZCI6IjAiLCJub2RlSWQiOiIxOTk4IiwiZGlzcGxheU5hbWUiOiJTa29raWUgU3VwZXJjZW50ZXIiLCJub2RlVHlwZSI6IlNUT1JFIiwiYWRkcmVzcyI6eyJwb3N0YWxDb2RlIjoiNjAwNzYiLCJhZGRyZXNzTGluZTEiOiIzNjI2IFRvdWh5IEF2ZSIsImNpdHkiOiJTa29raWUiLCJzdGF0ZSI6IklMIiwiY291bnRyeSI6IlVTIiwicG9zdGFsQ29kZTkiOiI2MDA3Ni0zOTQzIn0sImdlb1BvaW50Ijp7ImxhdGl0dWRlIjo0Mi4wMTI4MzQsImxvbmdpdHVkZSI6LTg3LjcxOTE0NX0sImlzR2xhc3NFbmFibGVkIjp0cnVlLCJzY2hlZHVsZWRFbmFibGVkIjp0cnVlLCJ1blNjaGVkdWxlZEVuYWJsZWQiOnRydWUsImFjY2Vzc1BvaW50cyI6W3siYWNjZXNzVHlwZSI6IkRFTElWRVJZX0FERFJFU1MifV0sImh1Yk5vZGVJZCI6IjE5OTgiLCJpc0V4cHJlc3NEZWxpdmVyeU9ubHkiOmZhbHNlLCJzdXBwb3J0ZWRBY2Nlc3NUeXBlcyI6WyJERUxJVkVSWV9BRERSRVNTIl19LCJpbnN0b3JlIjpmYWxzZSwicmVmcmVzaEF0IjoxNjY0MjgxNzM4MTkyLCJ2YWxpZGF0ZUtleSI6InByb2Q6djI6YTVmMDQyNDYtZDg2ZS00YTExLThiZmYtNTljMDFlZTMyNWZhIn0%3D; xptwj=cq:0e2ed25bc1085f8d8fbb:HQP235ntu0hfGVEdgOXv6qP/hqJMxcTAFC8onOGrWuI++re6k1lEZqV1l3R20zGSwMCTbnUfXSDfZdV7+vm8PH4Hyl++o+aeH0ChX1ALh2JtBV4qlz6JHK0Xs/2lrHVH7EUjSvziCPL/W16Sb2Gi791KihSn; akavpau_p2=1664260789~id=c2e8538506e8d8b287564cb3da705089; _px3=d19616aa68062ff7bc24af4f16d8481a11802aa3a5a36862894a96ea2ee6200e:AbT2rWfHrHrzbFanS3q1G1KSsbDT4+5lipdD2HS4MswCZUVYU280HwKyGpf+vomwEWCnE7hKb28nn+yODKrr1Q==:1000:EC/04I0gIK0bqZYZXn1vfCdJw+kESI/a1cnVJYlXZtyUwF2Aeql3lQYlRAosjWxdLJnaRhYfEmFU1KAsWbxaWHeXQaC5sb24JQL64gtLo+/Ld7d09VBFKgzOCW7a5Unuw32/LVQWNp5eDmZlR8fpZpVSrhiD/a00Ha2Baz38WkPUi8vzldto7JQ9CT1c98lTINfEwIKTW40QvNxN7Alp+w==; com.wm.reflector="reflectorid:0000000000000000000000@lastupd:1664260353000@firstcreate:1662382190811"; xptwg=3572087844:1E5DD8F71337E10:4E5E049:A56D94DB:EFA4D936:573F1A2A:; TS012768cf=011cd452e4366bd984d09e180731d54434ed73b1d4818449072f7431b7f025a871b2984cebaf27aabe7b241a75596b209fba850277; TS01a90220=011cd452e4366bd984d09e180731d54434ed73b1d4818449072f7431b7f025a871b2984cebaf27aabe7b241a75596b209fba850277; TS2a5e0c5c027=0837111aadab2000aa71223e372f8a6c6ab022eb0f9965023df0d2193e6112b56e10d4ba8b3e5eca080b925e3c113000ca9411c856e2e82d4ddc3eb16a411fd80eb1294b46d37605ca854f393a8418d194dcb11ec51705dd69b98ac44f44bf88; bm_sv=D4680B87B681B16021E6459EC484998B~YAAQhjYauNS571eDAQAA5KylfRF/mSwNZ3/gbOIVWMdIGziUdIViQf2xYQryoztdxi/4NcshgP9VKbn9G7pIISi1xfNRiJb7O74Z2AIfW3ViqKToUuHn6ftbKg6WFLj/2vj4fmPat77tRAMENX+Fhi6NgX2JE8sIBBOUVZcIl8ovkvWxMR1iGdps+VR8xq/H187f0D7zVNog2hX7PebG3Pgxn3/fVkpwimMuwjl3Uxg8iUVI0MPm+kjb8zUOYsbGShc=~1',
    #                     'device_profile_ref_id': 'Ynp5KUGtgsUnx-G6Dqn_I0PItsZxTSfMhtaq',
    #                     'origin': 'https://www.walmart.com',
    #                     'referer': 'https://www.walmart.com/browse/cell-phones/all-cell-phones/1105910_5296594?povid=web_categorypage_cellphones_lefthandnav&affinityOverride=default',
    #                     'traceparent': '00-70525a29403b5994ab4e0efcbe6093fa-b12645a7eb920087-00',
    #                     'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/105.0.0.0 Safari/537.36',
    #                     'wm_mp': 'true',
    #                     'wm_page_url': 'https://www.walmart.com/browse/cell-phones/all-cell-phones/1105910_5296594?povid=web_categorypage_cellphones_lefthandnav&affinityOverride=default',
    #                     'wm_qos.correlation_id': 'ufA9wfJX5rbJ4sRF-pYQ6l6kJ-GkCVV7QBXD',
    #                     'x-apollo-operation-name': 'Browse'}
    #
    #     return headers_new
