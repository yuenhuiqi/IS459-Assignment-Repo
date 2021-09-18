import scrapy

class HWZSpider(scrapy.Spider):
    name = 'hardwarezone'

    start_urls = [
        'https://forums.hardwarezone.com.sg/forums/pc-gaming.382/',
    ]

    def parse(self, response):

        # retrieving every single thread link
        for threadList in response.xpath('//div[has-class("structItemContainer-group js-threadList")]'):
            for thread in threadList.xpath('div[has-class("structItem structItem--thread js-inlineModContainer")]'):
                threadLink = thread.xpath('div/div[has-class("structItem-title")]/a/@href').get()
                yield response.follow(threadLink)

        # retrieving title, author & content from every post in the thread
        for post in response.xpath('//div[has-class("block-body js-replyNewMessageContainer")]'):
            yield {
                'title': response.xpath('//div[has-class("p-title")]/h1/text()').get(),
                'author': post.xpath('//a[has-class("username")]/text()').get(),
                'content': post.xpath('string(//div[@class=("bbWrapper")])').extract(),
            }

        # to scroll through & iterate through all threads & pages
        next_page = response.xpath('//a[@class=("pageNav-jump pageNav-jump--next")]/@href').get()
        if next_page is not None:
            yield response.follow(next_page)