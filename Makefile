# .PHONY: all gox aws-lambda gcp-cloudfunctions cli cli-linux cli-darwin cli-windows container format lint tidy test-setup test integration-reset integration-up integration-down integration-test container-release clean

# -----------------------------------------------------------------------------
#  TESTING
# -----------------------------------------------------------------------------

integration-reset: integration-down integration-up

integration-up: 
	(cd integration && docker-compose -f ./docker-compose.yml up -d)
	sleep 5

integration-down: 
	(cd integration && docker-compose -f ./docker-compose.yml down)
	rm -rf integration/.localstack
