from datetime import timedelta
from paysim_flow import etl_pipeline

# Immediate execution of the pipeline
if __name__ == "__main__":
    print("Starting ETL pipeline execution...")
    result = etl_pipeline()

    print(f"\nPipeline execution completed in {result['duration']}")
    print("\nOutput locations:")
    for layer, path in result.items():
        if 'path' in layer:
            print(f"{layer.split('_')[0]}: {path}")

    # Commented scheduled deployment configuration for future use
    """
    # Configure scheduled deployment
    deployment = etl_pipeline.serve(
        name="daily_etl_run",
        interval=timedelta(days=1),
        tags=["production"],
        version="1.0.0"
    )
    print(f"Deployment created: {deployment.deployment_id}")
    """