using CosmosDB.Models;
using Microsoft.Azure.Cosmos;
using Microsoft.Azure.Cosmos.Linq;

namespace CosmosDB.Services;

public class CosmosService : ICosmosService
{ 
    private readonly CosmosClient _client;
    public CosmosService()
    { 
        _client = new CosmosClient(
            connectionString: "AccountEndpoint=https://cdba-training.documents.azure.com:443/;AccountKey=MwwUjzpG5UWNMPSaNVMa4uaeMFMwDe42Og51zm8qRq70OhTf4qIVVzYpb82DDIyAfV2jqLvJ7Tx1ACDbb2yZxA==;"
        );
    }
    private Container container
    {
        get => _client.GetDatabase("cosmicworks").GetContainer("products");
    }
    public async Task<IEnumerable<Product>> RetrieveAllProductsAsync()
    {
        var queryable = container.GetItemLinqQueryable<Product>();
        using FeedIterator<Product> feed = queryable
            .Where(p => p.price < 2000m)
            .OrderByDescending(p => p.price)
            .ToFeedIterator();
        List<Product> results = new();
        while (feed.HasMoreResults)
        { 
            var response = await feed.ReadNextAsync();
            foreach (Product item in response)
            {
                results.Add(item);
            }
        }
        return results;
    }
    public async Task<IEnumerable<Product>> RetrieveActiveProductsAsync()
    { 
        string sql = """
        SELECT
            p.id,
            p.categoryId,
            p.categoryName,
            p.sku,
            p.name,
            p.description,
            p.price,
            p.tags
        FROM products p
        JOIN t IN p.tags
        WHERE ARRAY_CONTAINS(p.tags, @tagFilter)
        """;
        var query = new QueryDefinition(query: sql).WithParameter("@tagFilter", "70");
        using FeedIterator<Product> feed = container.GetItemQueryIterator<Product>(queryDefinition: query);

        List<Product> results = new();
        while (feed.HasMoreResults)
        {
            FeedResponse<Product> response = await feed.ReadNextAsync();
            foreach (Product item in response)
            {
                results.Add(item);
            }
        }
        return results;
    }
}